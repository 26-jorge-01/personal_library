# numeric_bias_rules.py
import numpy as np
import pandas as pd
import logging
from scipy.stats import boxcox
from sklearn.preprocessing import PowerTransformer, QuantileTransformer

logger = logging.getLogger(__name__)

def reduce_skewness_log(series: pd.Series):
    """
    Aplica una transformación logarítmica (log1p) para reducir el sesgo.
    Si existen valores <= 0, se ajusta la serie sumando |min|+1 automáticamente.
    """
    try:
        s = pd.to_numeric(series.dropna(), errors='coerce')
        min_val = s.min()
        s_adj = s + abs(min_val) + 1 if min_val <= 0 else s.copy()
        transformed = np.log1p(s_adj)
        # Se actualizan solo los índices donde hay datos originales
        series.update(pd.Series(transformed, index=s.index))
        return series, "Transformación log1p aplicada para reducir sesgo"
    except Exception as e:
        logger.error("Error en reduce_skewness_log: %s", str(e))
        return series, "Error en transformación log1p"

def reduce_skewness_boxcox(series: pd.Series):
    """
    Aplica la transformación Box–Cox para reducir el sesgo.
    Si hay valores <= 0, se ajusta la serie automáticamente.
    """
    try:
        s = pd.to_numeric(series.dropna(), errors='coerce')
        min_val = s.min()
        s_adj = s + abs(min_val) + 1 if min_val <= 0 else s.copy()
        transformed, _ = boxcox(s_adj)
        series.update(pd.Series(transformed, index=s.index))
        return series, "Transformación Box–Cox aplicada para reducir sesgo"
    except Exception as e:
        logger.error("Error en reduce_skewness_boxcox: %s", str(e))
        return series, "Error en transformación Box–Cox"

def reduce_skewness_yeojohnson(series: pd.Series):
    """
    Aplica la transformación Yeo–Johnson para reducir el sesgo.
    Permite valores negativos sin necesidad de ajuste.
    """
    try:
        pt = PowerTransformer(method="yeo-johnson")
        s = pd.to_numeric(series.dropna(), errors='coerce').values.reshape(-1, 1)
        transformed = pt.fit_transform(s).flatten()
        series.update(pd.Series(transformed, index=series.dropna().index))
        return series, "Transformación Yeo–Johnson aplicada para reducir sesgo"
    except Exception as e:
        logger.error("Error en reduce_skewness_yeojohnson: %s", str(e))
        return series, "Error en transformación Yeo–Johnson"

def quantile_normalization(series: pd.Series):
    """
    Aplica una transformación de cuantiles para igualar la distribución a una normal.
    Los parámetros se determinan automáticamente.
    """
    try:
        transformer = QuantileTransformer(output_distribution="normal", random_state=0)
        s = pd.to_numeric(series.dropna(), errors='coerce').values.reshape(-1, 1)
        transformed = transformer.fit_transform(s).flatten()
        series.update(pd.Series(transformed, index=series.dropna().index))
        return series, "Quantile normalization aplicada para reducir sesgo"
    except Exception as e:
        logger.error("Error en quantile_normalization: %s", str(e))
        return series, "Error en quantile normalization"
