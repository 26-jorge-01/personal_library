# datetime_bias_rules.py
import pandas as pd
import numpy as np
import logging
from utils.data_management.datetime import parse_to_timestamp

logger = logging.getLogger(__name__)

def reduce_temporal_skewness(series: pd.Series):
    """
    Aplica una transformación logarítmica a la diferencia en segundos entre cada fecha
    y la fecha mínima para reducir el sesgo en la distribución temporal.
    
    Se asegura que la serie resultante tenga la misma longitud e índices que la serie original.
    
    Args:
        series (pd.Series): Serie de pandas con datos de fecha.
    
    Returns:
        pd.Series: Serie de pandas transformada con la misma longitud e índices.
        str: Descripción de la acción realizada.
    """
    try:
        # Convertir cada elemento a Timestamp de forma robusta
        s = series.apply(parse_to_timestamp)
        if s.empty:
            return series, "Serie vacía, sin transformación temporal"
        
        # Calcular la fecha mínima ignorando NaT
        min_val = s.min()
        # Calcular la diferencia en segundos respecto a la fecha mínima
        diff = (s - min_val).dt.total_seconds()
        # Reemplazar los ceros por 1 para evitar log(0)
        diff = diff.replace(0, 1)
        log_diff = np.log(diff)
        # Normalizar la transformación
        norm = (log_diff - log_diff.min()) / (log_diff.max() - log_diff.min())
        total_range = (s.max() - min_val).total_seconds()
        transformed_seconds = norm * total_range
        transformed = min_val + pd.to_timedelta(transformed_seconds, unit='s')
        
        # Crear una copia de la serie original y actualizar solo los índices correspondientes a s
        out = series.copy()
        # Aquí usamos s.index ya que apply preserva el índice original
        out.loc[s.index] = transformed
        
        return out, "Transformación logarítmica aplicada para reducir sesgo temporal"
    except Exception as e:
        logger.error("Error en reduce_temporal_skewness: %s", str(e))
        return series, "Error en transformación temporal"

def cyclical_encoding(series: pd.Series):
    """
    Aplica una codificación cíclica a datos temporales y retorna una serie de fechas transformadas.
    
    La transformación se realiza de forma automática:
      - Si se detecta variación en el mes (más de un mes único), se asigna a cada fecha un
        año de referencia calculado automáticamente (por ejemplo, el modo o la mediana de los años presentes).
        Esto conserva el mes, día y componentes de tiempo, eliminando la tendencia temporal.
      
      - Si no hay variación en el mes pero sí en el día de la semana, se transforma cada fecha a
        una fecha canónica en la misma semana. La semana de referencia se determina a partir de la
        primera fecha válida, ajustándola automáticamente para que comience en lunes.
      
      - Si no se detecta variación cíclica, se retorna la serie original.
    
    Args:
        series (pd.Series): Serie de pandas con datos de fecha.
    
    Returns:
        pd.Series: Serie transformada a fecha canónica (dtype datetime) que captura el componente cíclico.
        str: Descripción de la transformación realizada.
    """
    try:
        # Convertir a datetime de forma robusta
        s = series.apply(parse_to_timestamp)
        
        # Función auxiliar para asegurar que dt sea un Timestamp
        def safe_to_timestamp(dt):
            if isinstance(dt, pd.Timestamp):
                return dt
            try:
                return pd.to_datetime(dt, errors='coerce')
            except Exception:
                return pd.NaT
        
        # Caso 1: Variación en mes (más de un mes único)
        if s.dt.month.nunique() > 1:
            # Calcular automáticamente el año de referencia usando el modo o la mediana
            years = s.dt.year.dropna()
            if not years.empty:
                ref_year = years.mode().iloc[0] if not years.mode().empty else int(years.median())
            else:
                ref_year = 2000  # fallback en caso de no haber datos válidos
            
            def transform_with_ref_year(dt):
                dt = safe_to_timestamp(dt)
                if pd.isnull(dt):
                    return pd.NaT
                return pd.Timestamp(
                    year = int(ref_year),
                    month = int(dt.month),
                    day = int(dt.day),
                    hour = int(dt.hour),
                    minute = int(dt.minute),
                    second = int(dt.second),
                    microsecond = int(dt.microsecond)
                )
            
            transformed = s.apply(transform_with_ref_year)
            message = f"Transformación canónica aplicada: año fijado automáticamente en {ref_year}."
        
        # Caso 2: Sin variación en mes pero con variación en día de la semana
        elif s.dt.dayofweek.nunique() > 1:
            # Tomar la primera fecha válida y calcular el lunes de esa semana
            first_date = s.dropna().iloc[0]
            first_date = safe_to_timestamp(first_date)
            ref_monday = first_date - pd.Timedelta(days=int(first_date.dayofweek))
            
            def transform_dayofweek(dt):
                dt = safe_to_timestamp(dt)
                if pd.isnull(dt):
                    return pd.NaT
                return ref_monday + pd.Timedelta(days=int(dt.dayofweek))
            
            transformed = s.apply(transform_dayofweek)
            message = f"Transformación canónica aplicada basada en días de la semana, usando referencia automática (lunes = {ref_monday.date()})."
        
        else:
            transformed = s
            message = "No se detectó variación cíclica; se retorna la serie original."
        
        # Aseguramos que se mantenga el mismo índice
        transformed.index = series.index
        return transformed, message
    except Exception as e:
        logger.error("Error en cyclical_encoding: %s", str(e))
        return series, "Error en codificación cíclica"
