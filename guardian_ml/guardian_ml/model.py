"""Guardar y cargar el modelo del guardián (local o en S3)."""
from __future__ import annotations

import json
import math
import os
import tempfile
import warnings
from functools import lru_cache
from pathlib import Path

import joblib
import numpy as np

from .features import FEATURES

BUNDLE_FILE = "model.joblib"
METRICS_FILE = "metrics.json"


def _split_s3(uri: str) -> tuple[str, str]:
    bucket, _, key = uri[5:].partition("/")
    return bucket, key.strip("/")


def save_bundle(bundle: dict, out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, out / BUNDLE_FILE)
    (out / METRICS_FILE).write_text(json.dumps(
        {k: v for k, v in bundle.items() if k != "model"}, indent=2, ensure_ascii=False))
    return out


@lru_cache(maxsize=4)
def load_bundle(uri: str) -> dict:
    """uri: carpeta local o s3://bucket/prefijo/ que contiene model.joblib."""
    if uri.startswith("s3://"):
        import boto3
        bucket, prefix = _split_s3(uri)
        tmp = Path(tempfile.mkdtemp(prefix="guardian-model-"))
        boto3.client("s3").download_file(bucket, f"{prefix}/{BUNDLE_FILE}", str(tmp / BUNDLE_FILE))
        uri = str(tmp)
    return joblib.load(Path(uri) / BUNDLE_FILE)


class _FastTrees:
    """Recorre los árboles del HistGradientBoostingClassifier en Python puro.

    predict_proba de scikit-learn tarda ~0,7 ms por fila (está pensado para
    lotes); en streaming se puntúa un reparto a la vez y esto tarda ~10 veces
    menos. Al cargar se verifica que dé exactamente lo mismo que scikit-learn.
    """

    def __init__(self, clf):
        self.base = float(np.ravel(clf._baseline_prediction)[0])
        self.trees = []
        for (pred,) in clf._predictors:
            n = pred.nodes
            self.trees.append((n["is_leaf"].tolist(), n["feature_idx"].tolist(),
                               n["num_threshold"].tolist(), n["left"].tolist(),
                               n["right"].tolist(), n["value"].tolist()))

    def proba(self, x: list[float]) -> float:
        raw = self.base
        for is_leaf, feat, thr, left, right, value in self.trees:
            i = 0
            while not is_leaf[i]:
                i = left[i] if x[feat[i]] <= thr[i] else right[i]
            raw += value[i]
        return 1.0 / (1.0 + math.exp(-raw))


class RiskModel:
    """Envuelve el modelo entrenado: features (dict) → riesgo entre 0 y 1."""

    def __init__(self, bundle: dict):
        self.bundle = bundle
        self.model = bundle["model"]
        self.features = bundle["features"]
        self.threshold = float(bundle["threshold"])
        self.version = bundle["version"]
        if self.features != FEATURES:
            raise ValueError("El modelo se entrenó con otras variables; vuelve a entrenarlo")
        self.fast = self._build_fast()

    def _build_fast(self):
        try:
            fast = _FastTrees(self.model)
            rng = np.random.default_rng(0)
            x = rng.uniform(0, 5, size=(200, len(self.features)))
            with warnings.catch_warnings():  # se le pasa numpy sin nombres de columnas a propósito
                warnings.simplefilter("ignore", UserWarning)
                ref = self.model.predict_proba(x)[:, 1]
            mine = np.array([fast.proba(row.tolist()) for row in x])
            return fast if np.allclose(ref, mine, atol=1e-9) else None
        except Exception:  # otra versión de scikit-learn: se usa predict_proba
            return None

    @classmethod
    def load(cls, uri: str) -> "RiskModel":
        return cls(load_bundle(uri))

    def risk(self, feats: dict) -> float:
        row = [float(feats[f]) for f in self.features]
        if self.fast is not None:
            return self.fast.proba(row)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            return float(self.model.predict_proba(np.array([row]))[0, 1])


def model_uri_from_env() -> str:
    return os.environ.get("MODEL_URI", "")
