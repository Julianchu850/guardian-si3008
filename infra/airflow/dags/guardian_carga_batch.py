"""Ruta batch: genera (opcional) un histórico con el simulador y sube a S3 las
dimensiones, la verdad terreno y los eventos históricos.

Qué sube, desde /opt/guardian-data/sim/<corrida>/:
- batch/   jugadores y catálogo (dimensiones)
- truth/   verdad terreno (perfiles y repartos forzados), solo para evaluación
- bronze/  eventos históricos generados en modo archivos

Las corridas del simulador por Kafka (etapa 2) también dejan su batch/ y truth/
en esa carpeta; este DAG los sube igual.
"""
from __future__ import annotations

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param, Variable, task

import guardian_lib as lib

DATA = "/opt/guardian-data/sim"

with DAG(
    dag_id="guardian_carga_batch",
    description="Histórico del simulador y dimensiones → S3 (ruta batch)",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["guardian", "batch"],
    params={
        "generar_historico": Param(True, type="boolean",
                                   description="Correr el simulador antes de subir"),
        "jugadores": Param(300, type="integer", minimum=10, maximum=5000),
        "dias": Param(7, type="integer", minimum=1, maximum=60),
        "politica": Param("split", enum=["split", "neutral", "predatory", "guarded"],
                          description="guarded = depredador con el guardián (usa models/guardian/latest)"),
        "semilla": Param(42, type="integer"),
        "fecha_inicio": Param("2026-11-16", type="string", format="date"),
    },
) as dag:

    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def debe_generar(params=None) -> bool:
        return bool(params["generar_historico"])

    generar = BashOperator(
        task_id="generar_historico",
        bash_command=(
            "cd /opt/guardian/simulator && python -m guardian_sim "
            "--players {{ params.jugadores }} --days {{ params.dias }} "
            "--policy {{ params.politica }} --seed {{ params.semilla }} "
            "--start-date {{ params.fecha_inicio }} --sink files --workers 2 "
            "--guardian-model {{ var.value.guardian_lake }}/models/guardian/latest "
            f"--out {DATA}/historico-{{{{ params.politica }}}}-s{{{{ params.semilla }}}}"
        ),
    )

    @task(trigger_rule="none_failed")
    def subir_a_s3() -> int:
        lake = Variable.get("guardian_lake")
        client = S3Hook(aws_conn_id="aws_default").get_conn()
        n = lib.sync_dir(client, DATA, lake)
        print(f"{n} archivos nuevos o actualizados en {lake}")
        return n

    debe_generar() >> generar >> subir_a_s3()
