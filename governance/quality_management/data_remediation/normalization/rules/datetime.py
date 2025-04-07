import pandas as pd
import logging
from utils.data_management.datetime import parse_to_timestamp

logger = logging.getLogger(__name__)

def normalize_datetime(series: pd.Series):
    """
    Normalización de fechas.

    Normalización de fechas es un método de escalamiento que transforma los valores de una serie
    para que tengan una distribución normal con media 0 y desviación estándar 1. Esto es útil
    para estandarizar datos que varían en diferentes escalas, como precios, altitudes, etc.
    
    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        

        # Aplicar la conversión a todos los elementos
        series = series.apply(parse_to_timestamp)
        return series, "Normalización de fechas aplicada"
    except Exception as e:
        logger.error("Error en normalize_datetime: %s", str(e))
        return series, "Error en normalización"