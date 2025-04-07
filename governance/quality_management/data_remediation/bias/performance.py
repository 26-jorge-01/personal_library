# bias_performance.py
import numpy as np
import pandas as pd
import logging
from utils.data_management.datetime import parse_to_timestamp
from scipy.stats import skew

logger = logging.getLogger(__name__)

def evaluate_numeric_bias(original_series: pd.Series, candidate_series: pd.Series):
    """
    Evalúa la reducción del sesgo en datos numéricos midiendo la diferencia en skewness.
    """
    try:
        orig_skew = abs(skew(original_series.dropna()))
        cand_skew = abs(skew(candidate_series.dropna()))
        reduction = orig_skew - cand_skew
        return {"original_skew": orig_skew, "candidate_skew": cand_skew, "reduction": reduction}
    except Exception as e:
        logger.error("Error en evaluate_numeric_bias: %s", str(e))
        return {"error": str(e)}

def evaluate_categorical_bias(original_series: pd.Series, candidate_series: pd.Series):
    """
    Evalúa la reducción del sesgo en datos categóricos midiendo la frecuencia del valor dominante.
    """
    try:
        orig_freq = original_series.dropna().value_counts(normalize=True).max() * 100
        cand_freq = candidate_series.dropna().value_counts(normalize=True).max() * 100
        reduction = orig_freq - cand_freq
        return {"original_dominant_freq": orig_freq, "candidate_dominant_freq": cand_freq, "reduction": reduction}
    except Exception as e:
        logger.error("Error en evaluate_categorical_bias: %s", str(e))
        return {"error": str(e)}

def evaluate_temporal_bias(original_series: pd.Series, candidate_series: pd.Series):
    """
    Evalúa la reducción del sesgo en datos temporales transformando cada elemento a Timestamp
    y midiendo una métrica basada en la dispersión (aquí, la media de la diferencia absoluta).
    """
    try:
        # Convertir cada elemento usando la función robusta
        orig_timestamps = original_series.apply(parse_to_timestamp)
        cand_timestamps = candidate_series.apply(parse_to_timestamp)
        
        # Convertir a valores numéricos (timestamps en segundos)
        orig_numeric = orig_timestamps.astype('int64') / 1e9
        cand_numeric = cand_timestamps.astype('int64') / 1e9
        
        # Calcular una medida de dispersión simple
        orig_dispersion = np.nanmean(np.abs(orig_numeric - np.nanmean(orig_numeric)))
        cand_dispersion = np.nanmean(np.abs(cand_numeric - np.nanmean(cand_numeric)))
        reduction = orig_dispersion - cand_dispersion
        
        return {"original_skew": abs(orig_dispersion), "candidate_skew": abs(cand_dispersion), "reduction": reduction}
    except Exception as e:
        logger.error("Error en evaluate_temporal_bias: %s", str(e))
        return {"error": str(e)}

def select_best_bias(candidates: dict, inferred_type: str):
    """
    Selecciona la variante de reducción de sesgo que logre la mayor mejora (mayor reducción).
    """
    best_variant = None
    best_reduction = -np.inf
    if inferred_type in ["integer", "float"]:
        for name, (series, metrics) in candidates.items():
            reduction = metrics.get("reduction", -np.inf)
            if reduction > best_reduction:
                best_reduction = reduction
                best_variant = name
    elif inferred_type == "string":
        for name, (series, metrics) in candidates.items():
            reduction = metrics.get("reduction", -np.inf)
            if reduction > best_reduction:
                best_reduction = reduction
                best_variant = name
    elif inferred_type == "datetime":
        for name, (series, metrics) in candidates.items():
            reduction = metrics.get("reduction", -np.inf)
            if reduction > best_reduction:
                best_reduction = reduction
                best_variant = name
    else:
        return None, {}
    return best_variant, {"reduction": best_reduction}

def evaluate_bias(original_series: pd.Series, candidate_series: pd.Series, inferred_type: str):
    if inferred_type in ["integer", "float"]:
        return evaluate_numeric_bias(original_series, candidate_series)
    elif inferred_type == "string":
        return evaluate_categorical_bias(original_series, candidate_series)
    elif inferred_type == "datetime":
        return evaluate_temporal_bias(original_series, candidate_series)
    else:
        return {"error": "Tipo de datos no soportado"}