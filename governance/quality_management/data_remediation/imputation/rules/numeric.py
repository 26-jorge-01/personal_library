import pandas as pd
import logging

logger = logging.getLogger(__name__)

def impute_with_median(series: pd.Series):
    """
    Imputa nulos con la mediana de la columna.
    
    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        median_value = series.median()
        series.fillna(median_value, inplace=True)
        return series, f"Imputados nulos con mediana ({median_value})"
    except Exception as e:
        logger.error("Error en impute_with_median: %s", str(e))
        return series, "Error en imputación con mediana"