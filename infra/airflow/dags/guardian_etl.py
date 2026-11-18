"""ETL del data lake en EMR: bronce → plata → oro.

1. detectar_fechas: qué fechas tienen datos nuevos en bronce (incremental), o
   las que se pasen a mano en el parámetro "fechas".
2. subir_codigo: copia los jobs de Spark a s3://…/code/.
3. crear_emr: levanta un clúster EMR (se apaga solo tras 1 h ocioso).
4. pasos_spark: bronce→plata, plata→oro y variables del guardián (gold/deal_features),
   con registro en el Glue Data Catalog.
5. esperar_*: sensores de cada paso.
6. apagar_emr: SIEMPRE se ejecuta, falle lo que falle (trigger_rule all_done).
7. validar_calidad: lee los reportes de calidad y falla si hay demasiados inválidos.
"""
from __future__ import annotations

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.operators.emr import (EmrAddStepsOperator,
                                                        EmrCreateJobFlowOperator)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.sdk import DAG, Param, Variable, task

import guardian_lib as lib

CODE_DIR = "/opt/guardian/spark/jobs"


def _s3():
    return S3Hook(aws_conn_id="aws_default").get_conn()


with DAG(
    dag_id="guardian_etl",
    description="Bronce → plata → oro en EMR, con calidad y catálogo Glue",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["guardian", "emr", "spark"],
    params={
        "fechas": Param("auto", type="string",
                        description="'auto' (incremental) o fechas AAAA-MM-DD separadas por coma"),
        "refrescar_dimensiones": Param(True, type="boolean"),
        "registrar_glue": Param(True, type="boolean"),
        "max_invalidos": Param(0.01, type="number", minimum=0, maximum=1,
                               description="proporción máxima de registros inválidos por fecha"),
    },
) as dag:

    @task
    def detectar_fechas(params=None) -> list[str]:
        if params["fechas"].strip().lower() != "auto":
            dates = sorted(d.strip() for d in params["fechas"].split(",") if d.strip())
        else:
            dates = lib.detect_dates(_s3(), Variable.get("guardian_lake"))
        if not dates:
            raise AirflowSkipException("No hay fechas nuevas en bronce")
        print("Fechas a procesar:", dates)
        return dates

    @task
    def subir_codigo() -> str:
        return lib.upload_code(_s3(), CODE_DIR, Variable.get("guardian_lake"))

    @task
    def config_cluster(ts_nodash=None) -> dict:
        return lib.job_flow_config(
            lake=Variable.get("guardian_lake"),
            release=Variable.get("guardian_emr_release", default="emr-7.2.0"),
            instance_type=Variable.get("guardian_emr_instance_type", default="m5.xlarge"),
            core_count=int(Variable.get("guardian_emr_core_count", default="2")),
            subnet=Variable.get("guardian_emr_subnet", default=""),
            key_name=Variable.get("guardian_emr_key", default=""),
            run_name=f"guardian-etl-{ts_nodash}",
        )

    @task
    def construir_pasos(dates: list[str], code_uri: str, params=None) -> list[dict]:
        return lib.spark_steps(Variable.get("guardian_lake"), code_uri, dates,
                               glue=params["registrar_glue"],
                               dims=params["refrescar_dimensiones"])

    fechas = detectar_fechas()
    codigo = subir_codigo()

    crear = EmrCreateJobFlowOperator(
        task_id="crear_emr",
        job_flow_overrides=config_cluster(),
        aws_conn_id="aws_default",
        emr_conn_id=None,
    )
    pasos = EmrAddStepsOperator(
        task_id="pasos_spark",
        job_flow_id=crear.output,
        steps=construir_pasos(fechas, codigo),
        aws_conn_id="aws_default",
    )
    esperar_plata = EmrStepSensor(
        task_id="esperar_bronce_plata",
        job_flow_id=crear.output,
        step_id="{{ ti.xcom_pull(task_ids='pasos_spark')[0] }}",
        aws_conn_id="aws_default", mode="reschedule", poke_interval=60, timeout=2 * 3600,
    )
    esperar_oro = EmrStepSensor(
        task_id="esperar_plata_oro",
        job_flow_id=crear.output,
        step_id="{{ ti.xcom_pull(task_ids='pasos_spark')[1] }}",
        aws_conn_id="aws_default", mode="reschedule", poke_interval=60, timeout=2 * 3600,
    )
    esperar_variables = EmrStepSensor(
        task_id="esperar_variables_guardian",
        job_flow_id=crear.output,
        step_id="{{ ti.xcom_pull(task_ids='pasos_spark')[2] }}",
        aws_conn_id="aws_default", mode="reschedule", poke_interval=60, timeout=2 * 3600,
    )

    @task(trigger_rule="all_done")
    def apagar_emr(ti=None) -> None:
        """Apaga el clúster pase lo que pase; si nunca se creó, no hace nada."""
        cluster_id = ti.xcom_pull(task_ids="crear_emr")
        if not cluster_id:
            print("No hay clúster que apagar")
            return
        EmrHook(aws_conn_id="aws_default", emr_conn_id=None).conn.terminate_job_flows(
            JobFlowIds=[cluster_id])
        print("Clúster apagado:", cluster_id)

    apagar = apagar_emr()

    @task
    def validar_calidad(dates: list[str], params=None) -> None:
        reports = lib.read_quality(_s3(), Variable.get("guardian_lake"), dates)
        for r in reports:
            print("CALIDAD", r)
        problems = lib.quality_problems(reports, dates, params["max_invalidos"])
        if problems:
            raise ValueError("Problemas de calidad:\n" + "\n".join(problems))

    [fechas, codigo] >> crear
    pasos >> esperar_plata >> esperar_oro >> esperar_variables >> apagar
    esperar_variables >> validar_calidad(fechas)
