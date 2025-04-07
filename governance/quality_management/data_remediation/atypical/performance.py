# outlier_performance.py
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def compute_outlier_percentage(series: pd.Series):
    """
    Calcula el porcentaje de outliers en una serie numérica usando el método IQR.
    Retorna el porcentaje de valores que se consideran outliers.
    """
    try:
        s = pd.to_numeric(series.dropna(), errors='coerce')
        if s.empty:
            return 0.0
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        if iqr == 0:
            return 0.0
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = s[(s < lower_bound) | (s > upper_bound)]
        return (len(outliers) / len(s)) * 100
    except Exception as e:
        logger.error("Error al calcular porcentaje de outliers: %s", str(e))
        return None

def evaluate_outlier_handling(original_series: pd.Series, candidate_series: pd.Series):
    """
    Evalúa el desempeño del manejo de outliers comparando el porcentaje de outliers
    antes y después del tratamiento. Retorna un diccionario con:
      - original_pct: Porcentaje de outliers en la serie original.
      - candidate_pct: Porcentaje de outliers en la serie tratada.
      - reduction: Diferencia (reducción) en porcentaje de outliers.
    """
    try:
        original_pct = compute_outlier_percentage(original_series)
        candidate_pct = compute_outlier_percentage(candidate_series)
        if original_pct is None or candidate_pct is None:
            return {"error": "Error en cálculo de outliers"}
        reduction = original_pct - candidate_pct
        return {"original_pct": original_pct, "candidate_pct": candidate_pct, "reduction": reduction}
    except Exception as e:
        logger.error("Error en evaluate_outlier_handling: %s", str(e))
        return {"error": str(e)}

def select_best_outlier_handling(candidates: dict):
    """
    Selecciona la mejor variante de manejo de outliers de entre las candidatas.
    'candidates' es un diccionario con clave: nombre de la variante,
    y valor: (candidate_series, performance_metrics).
    Se selecciona la variante con la mayor reducción en porcentaje de outliers.
    """
    best_variant = None
    best_reduction = -np.inf
    for name, (series, metrics) in candidates.items():
        reduction = metrics.get("reduction", -np.inf)
        if reduction > best_reduction:
            best_reduction = reduction
            best_variant = name
    return best_variant, {"reduction": best_reduction}
