import pandas as pd
import numpy as np
import re
import logging
from scipy.stats import entropy, median_abs_deviation

# Configuración básica del logger
logger = logging.getLogger(__name__)

def infer_data_type(series: pd.Series) -> str:
    """
    Infiere el tipo de dato de la serie usando heurísticas.
    Retorna: "integer", "float", "datetime", "boolean" o "string".
    """
    logger.debug("Iniciando inferencia de tipo para serie")
    if pd.api.types.is_bool_dtype(series):
        return "boolean"
    elif pd.api.types.is_integer_dtype(series):
        return "integer"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    else:
        try:
            pd.to_datetime(series.dropna(), errors='raise')
            return "datetime"
        except Exception:
            return "string"

def validate_data_type(series: pd.Series, inferred_type: str) -> str:
    """
    Valida si el tipo inferido es coherente con el contenido de la serie.
    Retorna un mensaje indicando si el tipo es adecuado o si hay posibles errores.
    """
    logger.debug("Validando tipo de dato inferido: %s", inferred_type)
    if pd.api.types.is_numeric_dtype(series):
        if inferred_type not in ["integer", "float"]:
            return f"Posible error: columna numérica con tipo inferido '{inferred_type}'."
    elif pd.api.types.is_datetime64_any_dtype(series):
        if inferred_type != "datetime":
            return f"Posible error: columna de fecha con tipo inferido '{inferred_type}'."
    elif pd.api.types.is_bool_dtype(series):
        if inferred_type != "boolean":
            return f"Posible error: columna booleana con tipo inferido '{inferred_type}'."
    return "Tipo asignado correcto"

def safe_nunique(series: pd.Series) -> int:
    """
    Calcula el número de elementos únicos en una serie.
    Si se detecta que algún elemento es un dict (o cualquier objeto no hashable),
    se convierte a su representación en string para poder calcular la unicidad.
    """
    try:
        return series.nunique(dropna=True)
    except TypeError:
        logger.warning("safe_nunique: Se encontró un elemento no hashable, convirtiendo a string.")
        return series.apply(lambda x: str(x) if isinstance(x, dict) else x).nunique(dropna=True)

def compute_basic_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas básicas comunes a todas las columnas:
      - Porcentaje de valores nulos
      - Porcentaje de duplicados
      - Conteo de valores únicos
    """
    logger.info("Calculando métricas básicas para la serie")
    total = len(series)
    metrics = {}
    metrics["null_percentage"] = series.isnull().mean() * 100 if total > 0 else None
    metrics["duplicate_percentage"] = (series.duplicated().sum() / total) * 100 if total > 0 else None
    metrics["unique_count"] = safe_nunique(series)
    logger.debug("Métricas básicas calculadas: %s", metrics)
    return metrics

def compute_numeric_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas para columnas numéricas.
    """
    logger.info("Calculando métricas numéricas")
    s = pd.to_numeric(series.dropna(), errors='coerce')
    if s.empty:
        logger.warning("Serie numérica vacía")
        return {}
    metrics = {}
    metrics["mean"] = s.mean()
    metrics["median"] = s.median()
    metrics["std"] = s.std()
    metrics["min"] = s.min()
    metrics["max"] = s.max()
    metrics["percentiles"] = {
        "25": s.quantile(0.25),
        "50": s.quantile(0.50),
        "75": s.quantile(0.75)
    }
    metrics["skewness"] = s.skew()
    metrics["kurtosis"] = s.kurtosis()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    if iqr > 0:
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outliers = s[(s < lower_bound) | (s > upper_bound)]
        metrics["outlier_percentage"] = (len(outliers) / len(s)) * 100
    else:
        metrics["outlier_percentage"] = 0.0
    logger.debug("Métricas numéricas: %s", metrics)
    return metrics

def compute_numeric_bias_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas adicionales para evaluar sesgos en columnas numéricas.
    """
    logger.info("Calculando métricas de sesgo para datos numéricos")
    s = pd.to_numeric(series.dropna(), errors='coerce')
    if s.empty:
        return {}
    metrics = {}
    skewness = s.skew()
    kurtosis = s.kurtosis()
    metrics["skewness"] = skewness
    metrics["kurtosis"] = kurtosis
    metrics["bias_flag"] = (abs(skewness) > 1) or (abs(kurtosis) > 3)
    logger.debug("Métricas de sesgo: %s", metrics)
    return metrics

def compute_datetime_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas para columnas de fecha.
    """
    logger.info("Calculando métricas para datos de fecha")
    try:
        dates = pd.to_datetime(series.dropna(), errors='coerce')
        if dates.empty:
            return {}
        sorted_dates = dates.sort_values()
        diffs = sorted_dates.diff().dropna().dt.total_seconds()
        metrics = {}
        metrics["median_time_diff"] = diffs.median() if not diffs.empty else None
        metrics["max_time_diff"] = diffs.max() if not diffs.empty else None
        if diffs.median() and diffs.max() > 2 * diffs.median():
            metrics["temporal_anomaly"] = (diffs.max() / diffs.median()) - 2
        else:
            metrics["temporal_anomaly"] = 0
        logger.debug("Métricas de fecha: %s", metrics)
        return metrics
    except Exception as e:
        logger.error("Error en compute_datetime_metrics: %s", str(e))
        return {"error": str(e)}

def compute_string_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas para columnas de texto.
    """
    logger.info("Calculando métricas para datos de texto")
    s = series.dropna().astype(str)
    metrics = {}
    metrics["unique_values"] = s.nunique()
    metrics["total_values"] = len(s)
    metrics["cardinality_ratio"] = s.nunique() / len(s) if len(s) > 0 else None
    metrics["avg_length"] = s.apply(len).mean()
    mode_val = s.mode()
    if not mode_val.empty:
        metrics["mode"] = mode_val.iloc[0]
        metrics["mode_frequency"] = (s == mode_val.iloc[0]).mean() * 100
    else:
        metrics["mode"] = None
        metrics["mode_frequency"] = None
    logger.debug("Métricas de texto: %s", metrics)
    return metrics

def compute_text_bias_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas para evaluar posibles sesgos en datos de texto.
    """
    logger.info("Calculando métricas de sesgo para datos de texto")
    s = series.dropna().astype(str)
    metrics = {}
    value_counts = s.value_counts(normalize=True)
    if not value_counts.empty:
        metrics["max_category_ratio"] = value_counts.iloc[0] * 100
        metrics["bias_flag"] = value_counts.iloc[0] > 0.80
    else:
        metrics["max_category_ratio"] = None
        metrics["bias_flag"] = False
    logger.debug("Métricas de sesgo en texto: %s", metrics)
    return metrics

def compute_boolean_metrics(series: pd.Series) -> dict:
    """
    Calcula métricas para columnas booleanas.
    """
    logger.info("Calculando métricas para datos booleanos")
    s = series.dropna().astype(bool)
    metrics = {}
    total = len(s)
    if total == 0:
        return {}
    true_count = s.sum()
    false_count = total - true_count
    metrics["true_percentage"] = (true_count / total) * 100
    metrics["false_percentage"] = (false_count / total) * 100
    logger.debug("Métricas booleanas: %s", metrics)
    return metrics

def compute_quality_score(metrics: dict, inferred_type: str) -> float:
    """
    Calcula un quality score (0 a 100) basado en las métricas combinadas.
    """
    logger.info("Calculando quality score para tipo %s", inferred_type)
    score = 100.0
    if "null_percentage" in metrics and metrics["null_percentage"] is not None:
        score -= metrics["null_percentage"] * 0.5
    if "duplicate_percentage" in metrics and metrics["duplicate_percentage"] is not None:
        score -= metrics["duplicate_percentage"] * 0.2
    if inferred_type in ["integer", "float"]:
        if "outlier_percentage" in metrics and metrics["outlier_percentage"] is not None:
            score -= metrics["outlier_percentage"] * 0.5
        if "skewness" in metrics and metrics["skewness"] is not None and abs(metrics["skewness"]) > 1:
            score -= (abs(metrics["skewness"]) - 1) * 10
    if inferred_type == "datetime":
        if "temporal_anomaly" in metrics and metrics["temporal_anomaly"] is not None:
            score -= metrics["temporal_anomaly"] * 5
    if inferred_type == "string":
        if "cardinality_ratio" in metrics and metrics["cardinality_ratio"] is not None and metrics["cardinality_ratio"] > 0.8:
            score -= (metrics["cardinality_ratio"] - 0.8) * 50
    logger.debug("Quality score calculado: %s", score)
    return max(score, 0)

def compute_entropy(series: pd.Series) -> float:
    """
    Calcula la entropía de Shannon de una serie para medir la diversidad de valores.
    """
    logger.info("Calculando entropía")
    counts = series.value_counts()
    probs = counts / counts.sum()
    result = entropy(probs)
    logger.debug("Entropía: %s", result)
    return result

def compute_gini_coefficient(series: pd.Series) -> float:
    """
    Calcula el coeficiente de Gini para una serie.
    """
    logger.info("Calculando coeficiente de Gini")
    arr = series.dropna()
    if arr.empty:
        return 0.0
    try:
        numeric = arr.astype(float)
        arr = np.sort(numeric.to_numpy())
        n = arr.size
        cumulative_values = np.cumsum(arr)
        total = cumulative_values[-1]
        if total == 0:
            return 0.0
        gini = (2 * np.sum((np.arange(1, n+1) * arr)) - (n + 1) * total) / (n * total)
        logger.debug("Gini calculado (numérico): %s", gini)
        return gini
    except Exception:
        counts = arr.value_counts().to_numpy()
        total = counts.sum()
        gini = 1 - np.sum((counts / total)**2)
        logger.debug("Gini calculado (frecuencias): %s", gini)
        return gini

def compute_mad_outlier_percentage(series: pd.Series) -> float:
    """
    Calcula el porcentaje de outliers usando la Mediana Absoluta de la Desviación (MAD).
    """
    logger.info("Calculando porcentaje de outliers con MAD")
    s = pd.to_numeric(series.dropna(), errors='coerce')
    if s.empty:
        return 0.0
    med = s.median()
    mad = median_abs_deviation(s)
    if mad == 0:
        return 0.0
    outliers = s[abs(s - med) > 3 * mad]
    result = (len(outliers) / len(s)) * 100
    logger.debug("Porcentaje de outliers (MAD): %s", result)
    return result

def detect_missing_pattern(df: pd.DataFrame) -> dict:
    """
    Analiza la distribución de valores nulos en el DataFrame y calcula la correlación.
    """
    logger.info("Detectando patrones en datos faltantes")
    missing_df = df.isnull().astype(int)
    correlation = missing_df.corr()
    logger.debug("Correlación de nulos: %s", correlation.to_dict())
    return correlation.to_dict()

def validate_format(series: pd.Series, pattern: str) -> dict:
    """
    Valida que los valores de la serie cumplan con el patrón regex dado.
    """
    logger.info("Validando formato con patrón: %s", pattern)
    regex = re.compile(pattern)
    results = series.dropna().astype(str).apply(lambda x: bool(regex.fullmatch(x)))
    invalid_count = (~results).sum()
    result = {"pattern": pattern, "invalid_count": int(invalid_count), "total_checked": len(results)}
    logger.debug("Resultado de validación de formato: %s", result)
    return result

def compute_coefficient_of_variation(series: pd.Series) -> float:
    """
    Calcula el coeficiente de variación para una serie numérica.
    """
    logger.info("Calculando coeficiente de variación")
    s = pd.to_numeric(series.dropna(), errors='coerce')
    if s.empty or s.mean() == 0:
        return 0.0
    result = s.std() / s.mean()
    logger.debug("Coeficiente de variación: %s", result)
    return result

class QualityReportEngine:
    """
    Motor de generación de reporte de calidad.
    """
    def __init__(self, df: pd.DataFrame, context: dict = None):
        self.df = df
        self.context = context or {}
        self.metric_registry = {
            "all": [compute_basic_metrics],
            "integer": [compute_numeric_metrics, compute_numeric_bias_metrics, compute_coefficient_of_variation, compute_mad_outlier_percentage],
            "float": [compute_numeric_metrics, compute_numeric_bias_metrics, compute_coefficient_of_variation, compute_mad_outlier_percentage],
            "datetime": [compute_datetime_metrics],
            "string": [compute_string_metrics, compute_text_bias_metrics, compute_entropy, compute_gini_coefficient],
            "boolean": [compute_boolean_metrics, compute_gini_coefficient]
        }
        logger.info("QualityReportEngine inicializado con %s columnas", len(self.df.columns))
    
    def register_metric(self, data_type: str, func):
        """
        Permite registrar funciones adicionales para un tipo de dato.
        """
        logger.info("Registrando métrica adicional para tipo %s: %s", data_type, func.__name__)
        if data_type in self.metric_registry:
            self.metric_registry[data_type].append(func)
        else:
            self.metric_registry[data_type] = [func]
    
    def generate_report(self) -> dict:
        """
        Genera el reporte integral de calidad.
        """
        logger.info("Generando reporte de calidad")
        report = {}
        global_scores = []
        for col in self.df.columns:
            logger.info("Procesando columna: %s", col)
            series = self.df[col]
            col_report = {}
            inferred_type = infer_data_type(series)
            col_report["inferred_type"] = inferred_type
            col_report["type_validation"] = validate_data_type(series, inferred_type)
            
            basic = compute_basic_metrics(series)
            col_report["basic_metrics"] = basic
            
            specific = {}
            funcs = self.metric_registry.get(inferred_type, [])
            for func in funcs:
                try:
                    res = func(series)
                    # Si el resultado no es un diccionario, encapsúlalo con la clave del nombre de la función.
                    if not isinstance(res, dict):
                        res = {func.__name__: res}
                    specific.update(res)
                except Exception as e:
                    logger.error("Error en función %s para columna %s: %s", func.__name__, col, str(e))
                    specific[func.__name__] = f"Error: {str(e)}"
            col_report["specific_metrics"] = specific
            
            all_metrics = {**basic, **specific}
            quality_score = compute_quality_score(all_metrics, inferred_type)
            col_report["quality_score"] = quality_score
            global_scores.append(quality_score)
            
            report[col] = col_report
        
        report["global"] = {
            "average_quality_score": np.mean(global_scores) if global_scores else None,
            "total_columns": len(self.df.columns)
        }
        logger.info("Reporte de calidad generado con éxito")
        return report
