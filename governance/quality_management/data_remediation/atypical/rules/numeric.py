import pandas as pd
import logging

logger = logging.getLogger(__name__)

def winsorize_iqr(series: pd.Series):
    """
    Winsorización usando IQR.
    
    Winsorización es un método de limpieza de datos que reemplaza los valores atípicos (outliers)
    con los valores más cercanos al rango intercuartil (IQR). Esto ayuda a prevenir la influencia
    de valores atípicos en el análisis estadístico.
    
    Parámetros:
      - series: Serie de pandas con los datos.
    
    Retorna:
      - series: Serie de pandas con los nulos imputados.
      - mensaje: Descripción de la acción realizada.
    """
    try:
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        series = series.clip(lower=lower_bound, upper=upper_bound)
        return series, f"Winsorizados outliers usando IQR: lower={lower_bound}, upper={upper_bound}"
    except Exception as e:
        logger.error("Error en winsorize_iqr: %s", str(e))
        return series, "Error en winsorization"