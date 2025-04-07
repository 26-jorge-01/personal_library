import numpy as np
import pandas as pd
import logging
from utils.data_management.datetime import parse_to_timestamp

logger = logging.getLogger(__name__)

def evaluate_numeric_imputation(original_series: pd.Series, imputed_series: pd.Series):
    """
    Evalúa el desempeño de la imputación en columnas numéricas usando RMSE y MAE.
    Se comparan los valores originales (donde están presentes) con los imputados.
    """
    try:
        mask = original_series.notnull()
        diff = original_series[mask] - imputed_series[mask]
        rmse = np.sqrt(np.mean(diff**2))
        mae = np.mean(np.abs(diff))
        return {"RMSE": rmse, "MAE": mae}
    except Exception as e:
        logger.error("Error en evaluate_numeric_imputation: %s", str(e))
        return {"error": str(e)}

def evaluate_string_imputation(original_series: pd.Series, imputed_series: pd.Series):
    """
    Evalúa la imputación en columnas de texto comparando el porcentaje de coincidencia exacta.
    """
    try:
        original = original_series.dropna().astype(str)
        imputed = imputed_series.loc[original.index].astype(str)
        matches = (original == imputed).sum()
        total = len(original)
        accuracy = matches / total * 100
        return {"Accuracy": accuracy}
    except Exception as e:
        logger.error("Error en evaluate_string_imputation: %s", str(e))
        return {"error": str(e)}

def evaluate_datetime_imputation(original_series: pd.Series, imputed_series: pd.Series):
    """
    Evalúa el desempeño de la imputación en columnas de fecha usando RMSE y MAE.
    Se comparan los valores originales (donde están presentes) con los imputados.
    La diferencia se mide en segundos para obtener métricas numéricas coherentes.
    
    Args:
        original_series (pd.Series): Serie de pandas con las fechas originales.
        imputed_series (pd.Series): Serie de pandas con las fechas imputadas.
    
    Returns:
        dict: Diccionario con RMSE y MAE (en segundos) de la imputación.
    """
    try:
        # Asegurarse de que ambas series sean de tipo datetime
        original_dt = original_series.apply(parse_to_timestamp)
        imputed_dt = imputed_series.apply(parse_to_timestamp)
        
        # Solo comparar donde el original tiene datos válidos
        mask = original_dt.notnull()
        if mask.sum() == 0:
            logger.warning("No hay valores originales válidos para evaluar.")
            return {"RMSE_seconds": 0, "MAE_seconds": 0}
        
        # Calcular la diferencia (Timedelta) y convertir a segundos
        diff = original_dt[mask] - imputed_dt[mask]
        diff_seconds = diff.dt.total_seconds()
        
        # Log de depuración: imprimir algunas diferencias
        logger.debug("Diferencias en segundos: %s", diff_seconds.head())
        
        rmse = np.sqrt(np.mean(diff_seconds**2))
        mae = np.mean(np.abs(diff_seconds))
        
        return {"RMSE_seconds": rmse, "MAE_seconds": mae}
    except Exception as e:
        logger.error("Error en evaluate_datetime_imputation: %s", str(e))
        return {"error": str(e)}



def select_best_imputation(candidates: dict, inferred_type: str):
    """
    Dado un diccionario de variantes candidatas (clave: nombre, valor: (candidate_series, performance_metrics)),
    selecciona la mejor variante en función de los criterios definidos:
      - Para datos numéricos: se selecciona la variante con menor RMSE.
      - Para datos de texto: se selecciona la variante con mayor Accuracy.
      - Para datos de fecha (datetime): se selecciona la variante con menor valor combinado de RMSE y MAE,
        calculado como el promedio de ambas métricas.
    
    Retorna el nombre de la mejor variante y sus métricas.
    """
    best_candidate = None
    best_metric = None

    if inferred_type in ["integer", "float"]:
        for name, (series, performance) in candidates.items():
            if "error" in performance:
                continue
            rmse = performance.get("RMSE", np.inf)
            if best_metric is None or rmse < best_metric:
                best_metric = rmse
                best_candidate = name
    elif inferred_type == "string":
        for name, (series, performance) in candidates.items():
            if "error" in performance:
                continue
            accuracy = performance.get("Accuracy", 0)
            if best_metric is None or accuracy > best_metric:
                best_metric = accuracy
                best_candidate = name
    elif inferred_type == "datetime":
        for name, (series, performance) in candidates.items():
            if "error" in performance:
                continue
            rmse = performance.get("RMSE_seconds", np.inf)
            mae = performance.get("MAE_seconds", np.inf)
            # Combinar ambas métricas (por ejemplo, tomando el promedio)
            combined_metric = (rmse + mae) / 2.0
            if best_metric is None or combined_metric < best_metric:
                best_metric = combined_metric
                best_candidate = name
    else:
        return None, {}

    return best_candidate, {"metric": best_metric}


def evaluate_imputation(original_series: pd.Series, imputed_series: pd.Series, inferred_type: str):
    """
    Función genérica para evaluar la imputación en función del tipo.
    """
    if inferred_type in ["integer", "float"]:
        return evaluate_numeric_imputation(original_series, imputed_series)
    elif inferred_type == "string":
        return evaluate_string_imputation(original_series, imputed_series)
    elif inferred_type == "datetime":
        return evaluate_datetime_imputation(original_series, imputed_series)
    else:
        return {"info": "No se define evaluación para este tipo"}
