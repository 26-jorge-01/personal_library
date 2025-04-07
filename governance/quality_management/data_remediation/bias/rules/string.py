# string_bias_rules.py
import pandas as pd
import numpy as np
import logging
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

def noop_string_bias(series: pd.Series):
    """
    Regla que no aplica ninguna transformación de sesgo.
    
    Args:
        series (pd.Series): Serie de pandas con los datos.
        
    Returns:
        pd.Series: Serie sin cambios.
        str: Descripción de la acción realizada.
    """
    return series, "No se aplicó transformación de sesgo (regla no-op)"

def group_rare_categories(series: pd.Series):
    """
    Agrupa automáticamente las categorías poco frecuentes en "Other".
    Se calcula el umbral como el percentil 25 de las frecuencias normalizadas.
    """
    try:
        series = series.astype(str)
        freq = series.value_counts(normalize=True)
        threshold = np.percentile(freq.values, 25)  # umbral automático: 25º percentil
        rare_categories = freq[freq < threshold].index
        series = series.apply(lambda x: "Other" if x in rare_categories else x)
        return series, f"Agrupadas categorías raras (umbral automático={threshold:.2f})"
    except Exception as e:
        logger.error("Error en group_rare_categories: %s", str(e))
        return series, "Error en agrupación de categorías raras"

def merge_similar_categories(series: pd.Series):
    """
    Fusiona categorías similares basándose en la similitud de secuencia.
    El umbral de similitud se define automáticamente como el percentil 75 de las similitudes entre pares.
    """
    try:
        series = series.astype(str)
        unique_vals = list(series.unique())
        similarities = []
        # Calcular todas las similitudes entre pares
        for i in range(len(unique_vals)):
            for j in range(i + 1, len(unique_vals)):
                sim = SequenceMatcher(None, unique_vals[i], unique_vals[j]).ratio()
                similarities.append(sim)
        if similarities:
            auto_threshold = np.percentile(similarities, 75)
        else:
            auto_threshold = 0.8  # valor por defecto
        merged = {}
        for i, val in enumerate(unique_vals):
            if val in merged:
                continue
            for other in unique_vals[i+1:]:
                sim = SequenceMatcher(None, val, other).ratio()
                if sim >= auto_threshold:
                    merged[other] = val
        series = series.apply(lambda x: merged.get(x, x))
        return series, f"Fusionadas categorías similares (umbral automático={auto_threshold:.2f})"
    except Exception as e:
        logger.error("Error en merge_similar_categories: %s", str(e))
        return series, "Error en fusión de categorías"
