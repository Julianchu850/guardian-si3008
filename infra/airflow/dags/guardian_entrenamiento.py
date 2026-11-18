"""Entrena el modelo del guardián y lo publica en S3 si pasa la validación.

1. descargar_variables: gold/deal_features de S3 a la EC2.
2. entrenar: validación temporal (primeros días entrenan, últimos días prueban),
   umbral elegido para el recall objetivo, comparación con la regla base.
3. validar_modelo: compuerta de calidad (PR-AUC mínimo y recall en prueba).
4. publicar_modelo: s3://…/models/guardian/v=<versión>/ y …/latest/.

El servicio guardian-scorer y el simulador (política guarded) leen latest/.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, Param, Variable, task

import guardian_lib as lib

WORK = Path("/opt/guardian-data/ml")


def _s3():
    return S3Hook(aws_conn_id="aws_default").get_conn()


with DAG(
    dag_id="guardian_entrenamiento",
    description="Entrena, valida y publica el modelo de riesgo del guardián",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["guardian", "ml"],
    params={
        "fechas": Param("todas", type="string",
                        description="'todas' o fechas AAAA-MM-DD separadas por coma"),
        "recall_objetivo": Param(0.9, type="number", minimum=0.5, maximum=1.0),
        "pr_auc_minimo": Param(0.3, type="number", minimum=0, maximum=1,
                               description="si el modelo no lo alcanza en prueba, no se publica"),
        "recall_minimo": Param(0.7, type="number", minimum=0, maximum=1),
    },
) as dag:

    @task
    def descargar_variables(run_id=None, params=None) -> str:
        dest = WORK / run_id.replace(":", "_").replace("+", "_") / "features"
        shutil.rmtree(dest, ignore_errors=True)
        dates = None if params["fechas"].strip().lower() == "todas" else \
            [d.strip() for d in params["fechas"].split(",") if d.strip()]
        n = lib.download_prefix(_s3(), Variable.get("guardian_lake"), "gold/deal_features", dest, dates)
        if n == 0:
            raise ValueError("No hay gold/deal_features en S3: corran primero guardian_etl")
        print(f"{n} archivos Parquet descargados en {dest}")
        return str(dest)

    @task
    def entrenar(features_dir: str, params=None) -> dict:
        from guardian_ml.model import save_bundle
        from guardian_ml.train import load, train
        bundle = train(load(features_dir), target_recall=params["recall_objetivo"])
        out = Path(features_dir).parent / "model"
        save_bundle(bundle, out)
        report = {k: v for k, v in bundle.items() if k != "model"}
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return {"dir": str(out), "version": bundle["version"], "test": bundle["test"],
                "baseline": bundle["baseline_rule_test"], "threshold": bundle["threshold"]}

    @task
    def validar_modelo(result: dict, params=None) -> dict:
        t = result["test"]
        problems = []
        if t["pr_auc"] < params["pr_auc_minimo"]:
            problems.append(f"PR-AUC {t['pr_auc']} < {params['pr_auc_minimo']}")
        if t["recall"] < params["recall_minimo"]:
            problems.append(f"recall {t['recall']} < {params['recall_minimo']}")
        print("Modelo:", t)
        print("Regla base:", result["baseline"])
        if problems:
            raise ValueError("El modelo no se publica: " + "; ".join(problems))
        return result

    @task
    def publicar_modelo(result: dict) -> list[str]:
        uris = lib.publish_model(_s3(), result["dir"], Variable.get("guardian_lake"), result["version"])
        print("Modelo publicado en:", uris)
        return uris

    publicar_modelo(validar_modelo(entrenar(descargar_variables())))
