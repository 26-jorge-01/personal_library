import base64
import time
import numpy as np
import pandas as pd
from governance.automation_and_monitoring.real_time_monitoring.metric_registry import MetricRegistry
from governance.automation_and_monitoring.automated_policies.engine_policy_autogen import get_or_create_policy, infer_column_type, define_integrity
import yaml
import logging

# Configuración global de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def is_base64_encoded(s: str) -> bool:
    """
    Verifica si una cadena 's' está en formato Base64.
    """
    try:
        if not isinstance(s, str) or len(s) == 0:
            return False
        if len(s) % 4 != 0:
            return False
        base64.b64decode(s, validate=True)
        return True
    except Exception:
        return False

# ===================================================
# Clase principal de monitoreo de calidad de datos
# ===================================================
class DataQualityMonitor:
    """
    Módulo para medir y monitorear la calidad de los datos en cada fase del pipeline.
    Se adapta a distintos escenarios (con/sin desagregación, flujos condicionales) y aplica las
    políticas de gobernanza definidas en un archivo YAML. Además, utiliza un registro central de
    métricas (MetricRegistry) para definir, reutilizar y parametrizar las medidas, y permite generar
    reportes y explicaciones detalladas del score final.
    """
    
    def __init__(self, df: pd.DataFrame, policy_filename: str, registry: MetricRegistry = None, historical_global_score: float = None):
        """
        Inicializa el monitor de calidad.
        
        :param df: DataFrame con los datos a evaluar.
        :param policy_filename: Ruta o nombre del archivo de política (ej. "s2_contracts.yaml").
        :param registry: Instancia de MetricRegistry para registrar métricas personalizadas.
        :param historical_global_score: (Opcional) Score global previo para análisis de drift.
        """
        self.df = df.copy()
        self.policy_filename = policy_filename
        self.historical_global_score = historical_global_score
        self.registry = registry if registry is not None else MetricRegistry()
        
        try:
            self.policy = get_or_create_policy(df, policy_filename)
            logging.info("Política cargada mediante get_or_create_policy.")
        except Exception as e:
            logging.warning(f"Fallo al cargar la política con get_or_create_policy: {e}. Se intenta cargar el YAML directamente.")
            with open(policy_filename, "r") as f:
                self.policy = yaml.safe_load(f)
        self.disaggregation_dimension = self.policy.get("disaggregation_dimension", None)
        self.primary_key = self.policy.get("primary_key", None)

    # Métodos para registrar medidas personalizadas
    def register_custom_field_measure(self, name: str, func: callable, phases: list = None) -> None:
        self.registry.register_field_metric(name, func, phases)

    def register_custom_pipeline_measure(self, name: str, func: callable, phases: list = None) -> None:
        self.registry.register_pipeline_metric(name, func, phases)

    # ===================================================
    # Cálculo a nivel de campo
    # ===================================================
    def compute_field_metrics(self, field: dict, series: pd.Series, current_phase: str = "ingestion") -> dict:
        """
        Calcula métricas para un campo, evaluando:
          - Nulos, coincidencia de tipo, duplicados y cardinalidad.
          - Estadísticos para numéricos, outliers mediante IQR y anomalías temporales.
          - Validación de seguridad (por ejemplo, encriptación).
          - Relaciones (placeholder).
          - Métricas personalizadas registradas (aplicadas solo si la fase actual está parametrizada).
        """
        metrics = {}
        metrics["null_percentage"] = series.isnull().mean() * 100
        inferred_type = infer_column_type(series)
        metrics["type_match"] = (inferred_type == field.get("type"))

        total = len(series)
        try:
            unique_count = series.nunique(dropna=True)
        except TypeError:
            unique_count = pd.Series(series.astype(str)).nunique(dropna=True)
        metrics["duplicate_percentage"] = (total - unique_count) / total * 100 if total > 0 else None
        metrics["uniqueness_rate"] = unique_count / total if total > 0 else None

        integrity = define_integrity(series)
        metrics["contains_outliers"] = bool(integrity.get("contains_outliers", False))

        field_type = field.get("type")
        if field_type in ["integer", "float"]:
            s = pd.to_numeric(series.dropna(), errors='coerce')
            if s.shape[0] > 0:
                metrics["mean"] = s.mean()
                metrics["median"] = s.median()
                metrics["std"] = s.std()
                metrics["percentiles"] = {
                    "25": s.quantile(0.25),
                    "50": s.quantile(0.50),
                    "75": s.quantile(0.75)
                }
                metrics["skewness"] = abs(s.skew())
                q1, q3 = s.quantile([0.25, 0.75])
                iqr = q3 - q1
                outlier_mask = (s < (q1 - 1.5 * iqr)) | (s > (q3 + 1.5 * iqr))
                metrics["outlier_percentage"] = outlier_mask.mean() * 100
            else:
                metrics["mean"] = metrics["median"] = metrics["std"] = metrics["skewness"] = None
                metrics["percentiles"] = {}
                metrics["outlier_percentage"] = None
        else:
            metrics["mean"] = metrics["median"] = metrics["std"] = metrics["skewness"] = None
            metrics["percentiles"] = {}
            metrics["outlier_percentage"] = None

        if field_type == "datetime" and series.dropna().shape[0] > 1:
            try:
                sorted_series = series.dropna().sort_values()
                diffs = sorted_series.diff().dropna().dt.total_seconds()
                median_diff = diffs.median()
                max_diff = diffs.max()
                metrics["temporal_anomaly"] = (max_diff / median_diff - 2) if (median_diff > 0 and max_diff > 2 * median_diff) else 0
            except Exception:
                metrics["temporal_anomaly"] = None
        else:
            metrics["temporal_anomaly"] = None

        if field_type == "string":
            s = series.dropna().apply(lambda x: x if isinstance(x, (int, float, str, bool)) else str(x))
            non_null_count = s.shape[0]
            unique_count = s.nunique()
            metrics["cardinality_ratio"] = (unique_count / non_null_count) if non_null_count > 0 else None
        else:
            metrics["cardinality_ratio"] = None

        security_req = field.get("security")
        if security_req == "encrypted" and field.get("type") == "string":
            non_null_vals = series.dropna().astype(str)
            metrics["security_compliant"] = non_null_vals.apply(is_base64_encoded).all() if non_null_vals.shape[0] > 0 else None
        elif security_req == "masked":
            non_null_vals = series.dropna().astype(str)
            metrics["security_compliant"] = non_null_vals.apply(lambda x: x.endswith("***") if isinstance(x, str) and len(x) > 3 else True).all() if non_null_vals.shape[0] > 0 else None
        else:
            metrics["security_compliant"] = True

        metrics["relational_compliance"] = "not evaluated" if field.get("relational_rules") else "n/a"

        # Ejecución de métricas personalizadas registradas, solo para la fase actual
        for name, measure in self.registry.get_field_metrics().items():
            if current_phase in measure["phases"]:
                try:
                    result = measure["func"](field, series, metrics)
                    metrics[f"custom_{name}"] = result
                except Exception as e:
                    logging.error(f"Error en métrica personalizada '{name}' para {field.get('field_name')}: {e}")
                    metrics[f"custom_{name}"] = None

        return metrics

    def compute_field_quality_score(self, field: dict, series: pd.Series, metrics: dict) -> float:
        """
        Calcula un score de calidad (0 a 100) para un campo, penalizando:
          - Incongruencia de tipo.
          - Porcentaje de nulos.
          - Presencia de outliers y sesgo elevado.
          - Anomalías temporales.
          - Cardinalidad excesiva.
          - Duplicados.
          - Incumplimiento en seguridad.
        """
        score = 100.0
        if not metrics.get("type_match", False):
            score -= 20
        score -= metrics.get("null_percentage", 0) * 0.5
        if field.get("type") in ["integer", "float"]:
            score -= metrics.get("outlier_percentage", 0) * 0.5
            if metrics.get("skewness", 0) > 1:
                score -= (metrics["skewness"] - 1) * 10
        if field.get("type") == "datetime":
            anomaly = metrics.get("temporal_anomaly")
            if anomaly is not None and anomaly > 0:
                score -= anomaly * 10
        if field.get("type") == "string" and metrics.get("cardinality_ratio", 0) and metrics["cardinality_ratio"] > 0.8:
            score -= (metrics["cardinality_ratio"] - 0.8) * 50
        score -= metrics.get("duplicate_percentage", 0) * 0.2
        if metrics.get("security_compliant") is False:
            score -= 15
        return max(score, 0)

    def generate_field_level_metrics(self, current_phase: str = "ingestion") -> dict:
        """
        Evalúa y devuelve un diccionario con métricas por cada campo definido en la política.
        Calcula además un score global a partir de los scores individuales.
        :param current_phase: Indica la fase en la que se está evaluando, para aplicar las métricas personalizadas.
        """
        metrics_dict = {}
        field_scores = []
        for field in self.policy.get("fields", []):
            col_name = field.get("field_name")
            if col_name not in self.df.columns:
                metrics_dict[col_name] = {"status": "missing"}
                continue
            series = self.df[col_name]
            field_metrics = self.compute_field_metrics(field, series, current_phase)
            quality_score = self.compute_field_quality_score(field, series, field_metrics)
            field_metrics["field_quality_score"] = quality_score
            metrics_dict[col_name] = field_metrics
            field_scores.append(quality_score)
        global_score = np.mean(field_scores) if field_scores else None
        metrics_dict["global"] = {
            "average_quality_score": global_score,
            "total_fields": len(self.policy.get("fields", []))
        }
        if self.historical_global_score is not None and global_score is not None:
            metrics_dict["global"]["drift_percentage"] = ((global_score - self.historical_global_score) / self.historical_global_score) * 100
        return metrics_dict

    # ===================================================
    # Fases del pipeline (evaluables on‑demand)
    # ===================================================
    def ingestion_phase(self) -> dict:
        """
        Fase de Ingesta: evalúa el cumplimiento global (ej., GDPR) y calcula las métricas de campo.
        """
        metrics = {}
        compliance = self.policy.get("compliance", {})
        metrics["gdpr_compliance"] = "Compliant" if "GDPR" in compliance.get("compliance_frameworks", []) else "Not Compliant"
        metrics["field_metrics"] = self.generate_field_level_metrics(current_phase="ingestion")
        security_settings = self.policy.get("enforcement_requirements", {})
        metrics["security_baseline"] = security_settings.get("security_baseline", "none")
        logging.info("Fase de Ingesta completada.")
        return metrics

    def disaggregation_phase(self) -> dict:
        """
        Fase de Desagregación: si se define una dimensión, agrupa datos y evalúa distribución e integridad.
        """
        metrics = {}
        if self.disaggregation_dimension and self.disaggregation_dimension in self.df.columns:
            groups = self.df.groupby(self.disaggregation_dimension)
            metrics["group_counts"] = groups.size().to_dict()
            if self.primary_key and self.primary_key in self.df.columns:
                metrics["primary_key_duplicate_pct"] = round(self.df[self.primary_key].duplicated().mean() * 100, 2)
            else:
                metrics["primary_key_duplicate_pct"] = "primary_key not defined"
        else:
            metrics["disaggregation"] = "No se aplica desagregación en este flujo"
        logging.info("Fase de Desagregación completada.")
        return metrics

    def cleaning_phase(self) -> dict:
        """
        Fase de Limpieza: elimina duplicados, rellena nulos según política y evalúa impacto.
        """
        result = {}
        pre_clean = self.df.isnull().mean() * 100
        df_clean = self.safe_drop_duplicates(self.df.copy())
        for col, fill_val in self.policy.get("fill_values", {}).items():
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna(fill_val)
        post_clean = df_clean.isnull().mean() * 100
        impact = (pre_clean - post_clean).round(2).to_dict()
        result["df_clean"] = df_clean
        result["post_clean_metrics"] = post_clean.to_dict()
        result["impact"] = impact
        self.df = df_clean.copy()
        logging.info("Fase de Limpieza completada.")
        return result

    def indexing_phase(self) -> dict:
        """
        Fase de Indexación: si se define una clave primaria, ordena el DataFrame y mide el tiempo invertido.
        """
        metrics = {}
        if self.primary_key and self.primary_key in self.df.columns:
            start = time.time()
            self.df.sort_values(by=self.primary_key, inplace=True)
            metrics["indexing_time_sec"] = round(time.time() - start, 4)
        else:
            metrics["indexing"] = "Indexación no aplicada (clave primaria no definida)"
        logging.info("Fase de Indexación completada.")
        return metrics

    def continuous_monitoring(self, threshold: float = 10.0) -> dict:
        """
        Monitoreo Continuo: compara el score global actual con uno histórico para detectar drift.
        """
        alerts = {}
        current_metrics = self.generate_field_level_metrics(current_phase="continuous_monitoring")
        current_score = current_metrics.get("global", {}).get("average_quality_score")
        if self.historical_global_score is not None and current_score is not None:
            drift = ((current_score - self.historical_global_score) / self.historical_global_score) * 100
            alerts["drift_percentage"] = drift
            alerts["alert"] = f"Drift detectado: {drift:.2f}%" if abs(drift) > threshold else "Drift dentro de límites aceptables"
            if abs(drift) > threshold:
                logging.warning(alerts["alert"])
        else:
            alerts["alert"] = "No se dispone de score histórico para comparar drift"
        logging.info("Monitoreo Continuo completado.")
        return alerts

    # ===================================================
    # Evaluación on‑demand y generación de reporte
    # ===================================================
    def evaluate_phase(self, phase: str) -> dict:
        """
        Evalúa on‑demand la fase especificada.
        :param phase: "ingestion", "disaggregation", "cleaning", "indexing" o "continuous_monitoring".
        """
        phases = {
            "ingestion": self.ingestion_phase,
            "disaggregation": self.disaggregation_phase,
            "cleaning": self.cleaning_phase,
            "indexing": self.indexing_phase,
            "continuous_monitoring": self.continuous_monitoring
        }
        if phase not in phases:
            raise ValueError(f"Fase '{phase}' no reconocida. Opciones: {list(phases.keys())}")
        return phases[phase]()

    def explain_scores(self) -> dict:
        """
        Genera un informe detallado por campo, indicando las métricas básicas y personalizadas usadas
        para calcular el score final de cada campo.
        """
        explanation = {}
        field_metrics = self.generate_field_level_metrics(current_phase="ingestion")
        for field, metrics in field_metrics.items():
            if field == "global":
                continue
            explanation[field] = {
                "base_metrics": {k: v for k, v in metrics.items() if not k.startswith("custom_") and k != "field_quality_score"},
                "custom_metrics": {k: v for k, v in metrics.items() if k.startswith("custom_")},
                "final_field_quality_score": metrics.get("field_quality_score")
            }
        return explanation
    
    def safe_drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convierte a string las celdas que contienen diccionarios para permitir la eliminación de duplicados.
        """
        df_mod = df.copy()
        for col in df_mod.columns:
            # Si alguna celda de la columna es un diccionario, se convierte a string
            if df_mod[col].apply(lambda x: isinstance(x, dict)).any():
                df_mod[col] = df_mod[col].apply(lambda x: str(x) if isinstance(x, dict) else x)
        return df_mod.drop_duplicates(keep="first")


    def generate_report(self) -> dict:
        """
        Genera un reporte consolidado que integra las métricas de todas las fases del pipeline,
        incluyendo las métricas personalizadas a nivel de pipeline.
        """
        report = {
            "ingestion": self.ingestion_phase(),
            "disaggregation": self.disaggregation_phase(),
            "cleaning": self.cleaning_phase(),
            "indexing": self.indexing_phase(),
            "continuous_monitoring": self.continuous_monitoring()
        }
        custom_results = {}
        for name, measure in self.registry.get_pipeline_metrics().items():
            if "report" in measure["phases"]:
                try:
                    custom_results[name] = measure["func"](self, report)
                except Exception as e:
                    logging.error(f"Error en métrica personalizada de pipeline '{name}': {e}")
                    custom_results[name] = None
        report["custom_pipeline_measures"] = custom_results
        report["timestamp"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return report