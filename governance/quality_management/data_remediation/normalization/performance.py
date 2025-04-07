# normalization_performance.py
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def evaluate_minmax_normalization(series: pd.Series):
    """
    Evalúa la normalización Min–Max. Se espera que los valores estén en [0,1].
    Calcula el porcentaje de valores fuera del rango y lo devuelve como error_rate.
    """
    try:
        total = len(series)
        below_zero = (series < 0).sum()
        above_one = (series > 1).sum()
        error_rate = (below_zero + above_one) / total
        return {"error_rate": error_rate}
    except Exception as e:
        logger.error("Error en evaluate_minmax_normalization: %s", str(e))
        return {"error": str(e)}

def evaluate_zscore_normalization(series: pd.Series):
    """
    Evalúa la normalización Z-score comparando la media y la desviación estándar
    del resultado con los valores ideales (0 y 1, respectivamente).
    Devuelve la suma de errores absolutos.
    """
    try:
        mean_val = series.mean()
        std_val = series.std()
        error = abs(mean_val - 0) + abs(std_val - 1)
        return {"error": error, "mean": mean_val, "std": std_val}
    except Exception as e:
        logger.error("Error en evaluate_zscore_normalization: %s", str(e))
        return {"error": str(e)}

def select_best_normalization(candidates: dict, target_method: str = None):
    """
    Selecciona la mejor variante de normalización de entre las candidatas.
    Cada candidato se evalúa por separado:
      - Si target_method es "minmax", se selecciona la variante con menor 'error_rate'.
      - Si target_method es "zscore", se selecciona la variante con menor 'error'.
    
    Si target_method es None, se realiza una selección conjunta:
      Se evalúan las variantes Min–Max y Z-score por separado y se elige la que
      presente el error más bajo (suponiendo escalas comparables).
    
    'candidates' es un diccionario con clave: nombre de la variante, y valor: (serie normalizada, performance_metrics).
    Devuelve una tupla: (nombre_de_la_variante, {"metric": valor}).
    """
    best_minmax, best_minmax_metric = None, np.inf
    best_zscore, best_zscore_metric = None, np.inf
    for name, (series, metrics) in candidates.items():
        if "minmax" in name:
            metric_val = metrics.get("error_rate", np.inf)
            if metric_val < best_minmax_metric:
                best_minmax_metric = metric_val
                best_minmax = name
        elif "zscore" in name:
            metric_val = metrics.get("error", np.inf)
            if metric_val < best_zscore_metric:
                best_zscore_metric = metric_val
                best_zscore = name

    if target_method == "minmax":
        return best_minmax, {"metric": best_minmax_metric}
    elif target_method == "zscore":
        return best_zscore, {"metric": best_zscore_metric}
    else:
        # Selección conjunta: elegir la variante con el error más bajo.
        if best_minmax_metric <= best_zscore_metric:
            overall = best_minmax
            overall_metric = best_minmax_metric
        else:
            overall = best_zscore
            overall_metric = best_zscore_metric
        return overall, {"metric": overall_metric}
        
def evaluate_normalization(series: pd.Series, method: str):
    """
    Evalúa la normalización aplicando la función correspondiente según el método.
    """
    if method == "normalize_minmax":
        return evaluate_minmax_normalization(series)
    elif method == "normalize_zscore":
        return evaluate_zscore_normalization(series)
    else:
        return {"info": "Evaluación no definida"}