"""Entrena y evalúa el modelo de riesgo del guardián.

Pregunta que responde: dado lo observable justo antes de un reparto, ¿qué tan
probable es que el juego lo esté manipulando (mano letal forzada)?

- Datos: gold/deal_features del mundo depredador (en el neutral no hay
  manipulación que aprender).
- Validación temporal: entrena con los primeros días y evalúa con los últimos,
  como se usaría en producción.
- Umbral: el más alto que logra el recall objetivo en un tramo de validación
  dentro del entrenamiento (el conjunto de prueba no se toca para elegirlo).
- Línea base: las reglas que describe el caso de referencia, aplicadas tal cual.

Uso: python -m guardian_ml.train <carpeta deal_features> <carpeta de salida>
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.inspection import permutation_importance
from sklearn.metrics import (average_precision_score, confusion_matrix, f1_score,
                             precision_score, recall_score, roc_auc_score)

from .features import FEATURES, LABEL
from .model import save_bundle

TRAIN_POLICIES = ("predatory",)


def load(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    df["dt"] = df["dt"].astype(str)
    df[LABEL] = df[LABEL].fillna(False).astype(bool)
    return df[df.policy.isin(TRAIN_POLICIES)].reset_index(drop=True)


def split_by_time(df: pd.DataFrame, test_frac: float = 0.3) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Las fechas más recientes que suman al menos test_frac de las filas van a prueba."""
    counts = df.groupby("dt").size().sort_index()
    if len(counts) < 2:
        raise ValueError("Se necesitan al menos 2 fechas para separar entrenamiento y prueba")
    share = counts[::-1].cumsum() / counts.sum()
    test_dates = set(share[share.shift(fill_value=0) < test_frac].index)
    test_dates.discard(counts.index[0])
    train_df, test_df = df[~df.dt.isin(test_dates)], df[df.dt.isin(test_dates)]
    for name, part in (("entrenamiento", train_df), ("prueba", test_df)):
        if part[LABEL].sum() == 0:
            raise ValueError(f"No hay repartos forzados en {name}: simulen más jugadores o días")
    return train_df, test_df


def baseline_rule(df: pd.DataFrame) -> np.ndarray:
    """Las reglas del caso de referencia, escritas a mano."""
    ad = (df.rewarded_last3_completed >= 3) & (df.ecpm_ratio >= 3.5) & (df.above_record == 1)
    margin = (df.game_num > 1) & (df.score_ratio > 1.10)
    return (ad | margin).to_numpy()


def pick_threshold(y: np.ndarray, p: np.ndarray, target_recall: float) -> float:
    for t in np.unique(np.round(p, 6))[::-1]:
        if recall_score(y, p >= t, zero_division=0) >= target_recall:
            return float(t)
    return float(p.min())


def _metrics(y, pred, p=None) -> dict:
    tn, fp, fn, tp = confusion_matrix(y, pred, labels=[False, True]).ravel()
    out = {"precision": round(precision_score(y, pred, zero_division=0), 4),
           "recall": round(recall_score(y, pred, zero_division=0), 4),
           "f1": round(f1_score(y, pred, zero_division=0), 4),
           "confusion": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
           "alert_rate": round(float(np.mean(pred)), 5)}
    if p is not None:
        out["pr_auc"] = round(average_precision_score(y, p), 4)
        out["roc_auc"] = round(roc_auc_score(y, p), 4)
    return out


def train(df: pd.DataFrame, target_recall: float = 0.9, seed: int = 0) -> dict:
    train_df, test_df = split_by_time(df)
    fit_df, val_df = split_by_time(train_df, 0.25) if train_df.dt.nunique() >= 2 else (train_df, train_df)

    def model():
        return HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=15,
                                              l2_regularization=1.0, random_state=seed)

    # 1) umbral elegido en validación
    m = model().fit(fit_df[FEATURES], fit_df[LABEL])
    threshold = pick_threshold(val_df[LABEL].to_numpy(), m.predict_proba(val_df[FEATURES])[:, 1],
                               target_recall)
    # 2) modelo final con todo el entrenamiento, evaluado en prueba
    clf = model().fit(train_df[FEATURES], train_df[LABEL])
    y = test_df[LABEL].to_numpy()
    p = clf.predict_proba(test_df[FEATURES])[:, 1]
    imp = permutation_importance(clf, test_df[FEATURES], y, scoring="average_precision",
                                 n_repeats=5, random_state=seed)
    importance = sorted(((f, round(float(v), 4)) for f, v in zip(FEATURES, imp.importances_mean)),
                        key=lambda kv: -kv[1])
    return {
        "model": clf,
        "features": FEATURES,
        "threshold": round(threshold, 6),
        "version": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "target_recall": target_recall,
        "data": {"train_dates": sorted(train_df.dt.unique()), "test_dates": sorted(test_df.dt.unique()),
                 "train_rows": len(train_df), "train_positives": int(train_df[LABEL].sum()),
                 "test_rows": len(test_df), "test_positives": int(y.sum())},
        "test": _metrics(y, p >= threshold, p),
        "baseline_rule_test": _metrics(y, baseline_rule(test_df)),
        "importance": importance,
    }


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("features")
    ap.add_argument("out")
    ap.add_argument("--target-recall", type=float, default=0.9)
    args = ap.parse_args(argv)
    bundle = train(load(args.features), args.target_recall)
    save_bundle(bundle, args.out)
    json.dump({k: v for k, v in bundle.items() if k != "model"}, sys.stdout, indent=2, ensure_ascii=False)
    print()


if __name__ == "__main__":
    main()
