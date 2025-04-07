import pandas as pd
import logging
from utils.data_management.datetime import parse_to_timestamp

logger = logging.getLogger(__name__)

def impute_mode_date(series: pd.Series):
    """
    Imputa nulos y valores por defecto con la fecha más común (moda) de la columna.

    Args:
        series (pd.Series): Serie de pandas con los datos.

    Returns:
        pd.Series: Serie de pandas con los nulos imputados.
        str: Descripción de la acción realizada.
    """
    try:
        # Asegurarse de que la serie esté en formato datetime
        series = series.apply(parse_to_timestamp)
        
        # Filtrar valores válidos (excluyendo nulos y la fecha por defecto)
        non_default = series[~(series.eq(pd.Timestamp('1970-01-01')) | series.isnull())]
        
        # Si no hay valores válidos, se mantiene la fecha por defecto
        if non_default.empty:
            mode_date = pd.Timestamp('1970-01-01')
        else:
            mode_date = non_default.mode().iloc[0]

        # Imputar aquellos valores que sean iguales a la fecha por defecto
        series = series.mask(series.eq(pd.Timestamp('1970-01-01')), mode_date)
        
        return series, f"Imputados nulos y valores por defecto con moda de fecha ({mode_date})"
    except Exception as e:
        logger.error("Error al imputar fecha por defecto: %s", str(e))
        return series, f"Error al imputar fecha por defecto: {str(e)}"


def impute_default_date(series: pd.Series):
    """
    Imputa nulos con una fecha por defecto (1970-01-01).

    Args:
        series (pd.Series): Serie de pandas con los datos.

    Returns:
        pd.Series: Serie de pandas con los nulos imputados.
        str: Descripción de la acción realizada.
    """
    try:
        series.fillna(pd.Timestamp('1970-01-01'), inplace=True)
        return series, "Imputados nulos con fecha por defecto (1970-01-01)"
    except Exception as e:
        logger.error("Error al imputar fecha por defecto: %s", str(e))
        return series, f"Error al imputar fecha por defecto: {str(e)}"