import pandas as pd
import re
import numpy as np

def rule_positive(df: pd.DataFrame, field: str, **params) -> str:
    try:
        if (df[field].astype(float) < 0).any():
            return f"El campo '{field}' contiene valores negativos."
    except Exception as e:
        return f"Error en rule_positive para '{field}': {e}"
    return ""

def rule_not_future(df: pd.DataFrame, field: str, **params) -> str:
    try:
        dates = pd.to_datetime(df[field], errors='coerce')
        if dates.gt(pd.Timestamp.now()).any():
            return f"El campo '{field}' contiene fechas futuras."
    except Exception as e:
        return f"Error en rule_not_future para '{field}': {e}"
    return ""

def rule_not_null(df: pd.DataFrame, field: str, **params) -> str:
    if df[field].isnull().any():
        return f"El campo '{field}' contiene valores nulos."
    return ""

def rule_unique(df: pd.DataFrame, field: str, **params) -> str:
    if df[field].duplicated().any():
        return f"El campo '{field}' contiene valores duplicados."
    return ""

def rule_range(df: pd.DataFrame, field: str, min=None, max=None, **params) -> str:
    try:
        series = df[field].astype(float)
        if min is not None and (series < min).any():
            return f"El campo '{field}' contiene valores menores que {min}."
        if max is not None and (series > max).any():
            return f"El campo '{field}' contiene valores mayores que {max}."
    except Exception as e:
        return f"Error en rule_range para '{field}': {e}"
    return ""

def rule_allowed_values(df: pd.DataFrame, field: str, allowed_values=None, **params) -> str:
    if allowed_values is None:
        return ""
    invalid = df[~df[field].isin(allowed_values)]
    if not invalid.empty:
        return f"El campo '{field}' contiene valores fuera del conjunto permitido."
    return ""

def rule_regex(df: pd.DataFrame, field: str, pattern=None, **params) -> str:
    if pattern is None:
        return ""
    regex = re.compile(pattern)
    invalid = df[~df[field].astype(str).apply(lambda x: bool(regex.fullmatch(x)))]
    if not invalid.empty:
        return f"El campo '{field}' contiene valores que no cumplen con el patrón {pattern}."
    return ""

def rule_string_length(df: pd.DataFrame, field: str, min_length=None, max_length=None, **params) -> str:
    try:
        lengths = df[field].astype(str).apply(len)
        if min_length is not None and (lengths < min_length).any():
            return f"El campo '{field}' tiene valores con menos de {min_length} caracteres."
        if max_length is not None and (lengths > max_length).any():
            return f"El campo '{field}' tiene valores con más de {max_length} caracteres."
    except Exception as e:
        return f"Error en rule_string_length para '{field}': {e}"
    return ""

def rule_date_order(df: pd.DataFrame, fields: list, **params) -> str:
    # 'fields' debe ser una lista de dos: [campo_inicio, campo_fin]
    if not isinstance(fields, list) or len(fields) != 2:
        return "La regla date_order requiere una lista de dos campos."
    start_field, end_field = fields
    try:
        start_dates = pd.to_datetime(df[start_field], errors='coerce')
        end_dates = pd.to_datetime(df[end_field], errors='coerce')
        if (start_dates > end_dates).any():
            return f"El campo '{start_field}' es posterior a '{end_field}' en algunos registros."
    except Exception as e:
        return f"Error en rule_date_order para los campos {fields}: {e}"
    return ""

def rule_foreign_key(df: pd.DataFrame, field: str, reference_values=None, **params) -> str:
    if reference_values is None:
        return ""
    invalid = df[~df[field].isin(reference_values)]
    if not invalid.empty:
        return f"El campo '{field}' contiene valores que no están en el conjunto de referencia."
    return ""

def rule_outlier(df: pd.DataFrame, field: str, threshold=3, **params) -> str:
    try:
        series = df[field].astype(float)
        mean = series.mean()
        std = series.std()
        outliers = series[np.abs(series - mean) > threshold * std]
        if not outliers.empty:
            return f"El campo '{field}' contiene outliers (más de {threshold} desviaciones estándar)."
    except Exception as e:
        return f"Error en rule_outlier para '{field}': {e}"
    return ""

def rule_distribution(df: pd.DataFrame, field: str, baseline_distribution=None, **params) -> str:
    # Compara la media y la desviación estándar con una distribución base.
    if baseline_distribution is None:
        return ""
    try:
        series = df[field].astype(float)
        mean = series.mean()
        std = series.std()
        base_mean = baseline_distribution.get("mean", mean)
        base_std = baseline_distribution.get("std", std)
        if abs(mean - base_mean) > base_std:
            return f"La distribución del campo '{field}' difiere significativamente de la base."
    except Exception as e:
        return f"Error en rule_distribution para '{field}': {e}"
    return ""
