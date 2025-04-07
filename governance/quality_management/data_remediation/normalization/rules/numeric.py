import pandas as pd
import logging
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)

def normalize_minmax(series: pd.Series):
    """
    Normalización Min-Max.

    Normalización Min-Max es un método de escalamiento que transforma los valores de una serie
    para que estén en un rango específico (generalmente [0, 1]). Esto es útil para estandarizar
    datos que varían en diferentes escalas, como precios, altitudes, etc.
    
    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        scaler = MinMaxScaler()
        normalized = pd.Series(scaler.fit_transform(series.values.reshape(-1, 1)).flatten(), index=series.index)
        return normalized, "Normalización Min-Max aplicada"
    except Exception as e:
        logger.error("Error en normalize_minmax: %s", str(e))
        return series, "Error en normalización"

def normalize_zscore(series: pd.Series):
    """
    Normalización Z-score.

    Normalización Z-score es un método de escalamiento que transforma los valores de una serie
    para que tengan una distribución normal con media 0 y desviación estándar 1. Esto es útil
    para estandarizar datos que varían en diferentes escalas, como precios, altitudes, etc.
    
    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        numeric = pd.to_numeric(series.dropna(), errors='coerce')
        mean_val = numeric.mean()
        std_val = numeric.std()
        if std_val == 0:
            return series, "Coeficiente de variación 0, sin normalización Z-score"
        normalized = (numeric - mean_val) / std_val
        series.update(normalized)
        return series, f"Normalización Z-score aplicada (mean={mean_val:.2f}, std={std_val:.2f})"
    except Exception as e:
        logger.error("Error en normalize_zscore: %s", str(e))
        return series, "Error en normalización"