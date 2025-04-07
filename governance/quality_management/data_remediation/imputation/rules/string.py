import pandas as pd
import logging

logger = logging.getLogger(__name__)

def impute_empty_string(series: pd.Series):
    """
    Imputa nulos con cadena vacía.

    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        series.fillna("", inplace=True)
        return series, "Imputados nulos con cadena vacía"
    except Exception as e:
        logger.error("Error en impute_empty_string: %s", str(e))
        return series, "Error en imputación de string"