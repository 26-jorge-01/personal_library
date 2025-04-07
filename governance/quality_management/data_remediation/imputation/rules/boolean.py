import pandas as pd
import logging

logger = logging.getLogger(__name__)

def impute_boolean_false(series: pd.Series):
    """
    Imputa nulos con False.
    
    Args:
        series (pd.Series): Serie de pandas con los datos.
    
    Returns:
        pd.Series: Serie de pandas con los nulos imputados.
        str: Descripción de la acción realizada.
    """
    try:
        series.fillna(False, inplace=True)
        return series, "Imputados nulos con False"
    except Exception as e:
        logger.error("Error en impute_boolean_false: %s", str(e))
        return series, "Error en imputación booleana"

def impute_boolean_true(series: pd.Series):
    """
    Imputa nulos con True.
    
    Args:
        series (pd.Series): Serie de pandas con los datos.
    
    Returns:
        pd.Series: Serie de pandas con los nulos imputados.
        str: Descripción de la acción realizada.
    """
    try:
        series.fillna(True, inplace=True)
        return series, "Imputados nulos con True"
    except Exception as e:
        logger.error("Error en impute_boolean_true: %s", str(e))
        return series, "Error en imputación booleana"