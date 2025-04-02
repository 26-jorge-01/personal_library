import base64
import numpy as np
import pandas as pd
from governance.engine_policy_autogen import get_or_create_policy, infer_column_type, define_integrity

def is_base64_encoded(s):
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

class MetricsEngine:
    def __init__(self, df: pd.DataFrame, policy_filename: str, historical_global_score: float = None):
        """
        Inicializa el MetricsEngine con el dataset, la política correspondiente y,
        opcionalmente, una métrica global histórica para análisis de drift.
        
        :param df: DataFrame con los datos a evaluar.
        :param policy_filename: Nombre del archivo de política (ej. "s2_contracts.yaml").
        :param historical_global_score: (Opcional) Quality score global anterior para drift.
        """
        self.df = df.copy()
        self.policy = get_or_create_policy(df, policy_filename)
        self.historical_global_score = historical_global_score

    def compute_field_metrics(self, field, series):
        """
        Calcula métricas robustas para un campo en base a:
          - Porcentaje de valores nulos.
          - Verificación de tipo.
          - Unicidad y duplicados.
          - Integridad y outliers.
          - Estadísticos descriptivos para campos numéricos.
          - Anomalías temporales para campos datetime.
          - Cardinalidad para campos categóricos.
          - Validación de seguridad.
          - (Placeholder) Relaciones entre campos.
        """
        metrics = {}
        # 1. Porcentaje de valores nulos.
        null_pct = series.isnull().mean() * 100
        metrics["null_percentage"] = null_pct

        # 2. Tipo de dato: coincidencia entre inferido y esperado.
        inferred_type = infer_column_type(series)
        metrics["type_match"] = (inferred_type == field.get("type"))

        # 3. Unicidad y duplicados.
        total = len(series)
        try:
            unique_count = series.nunique(dropna=True)
        except TypeError:
            unique_count = pd.Series(series.astype(str)).nunique(dropna=True)
        duplicate_pct = (total - unique_count) / total * 100 if total > 0 else None
        metrics["duplicate_percentage"] = duplicate_pct
        metrics["uniqueness_rate"] = unique_count / total if total > 0 else None

        # 4. Integridad y outliers (usando define_integrity).
        integrity = define_integrity(series)
        metrics["contains_outliers"] = bool(integrity.get("contains_outliers", False))

        # 5. Estadísticos descriptivos para numéricos.
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
                q1 = s.quantile(0.25)
                q3 = s.quantile(0.75)
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

        # 6. Anomalías temporales en campos datetime.
        if field_type == "datetime" and series.dropna().shape[0] > 1:
            try:
                sorted_series = series.dropna().sort_values()
                diffs = sorted_series.diff().dropna().dt.total_seconds()
                median_diff = diffs.median()
                max_diff = diffs.max()
                if median_diff > 0 and max_diff > 2 * median_diff:
                    anomaly_factor = (max_diff / median_diff) - 2
                    metrics["temporal_anomaly"] = anomaly_factor
                else:
                    metrics["temporal_anomaly"] = 0
            except Exception:
                metrics["temporal_anomaly"] = None
        else:
            metrics["temporal_anomaly"] = None

        # 7. Cardinalidad en campos categóricos (string).
        if field_type == "string":
            # Convertir a string aquellos elementos que no sean hashables
            s = series.dropna().apply(lambda x: x if isinstance(x, (int, float, str, bool)) else str(x))
            non_null_count = s.shape[0]
            unique_count = s.nunique()
            metrics["cardinality_ratio"] = (unique_count / non_null_count) if non_null_count > 0 else None
        else:
            metrics["cardinality_ratio"] = None

        # 8. Seguridad y cumplimiento:
        security_req = field.get("security")
        if security_req == "encrypted" and field.get("type") == "string":
            non_null_vals = series.dropna().astype(str)
            if non_null_vals.shape[0] > 0:
                compliant = non_null_vals.apply(is_base64_encoded).all()
                metrics["security_compliant"] = compliant
            else:
                metrics["security_compliant"] = None
        elif security_req == "masked":
            non_null_vals = series.dropna().astype(str)
            if non_null_vals.shape[0] > 0:
                compliant = non_null_vals.apply(lambda x: x.endswith("***") if isinstance(x, str) and len(x) > 3 else True).all()
                metrics["security_compliant"] = compliant
            else:
                metrics["security_compliant"] = None
        else:
            metrics["security_compliant"] = True

        # 9. Validación de relaciones entre campos (placeholder).
        if field.get("relational_rules"):
            metrics["relational_compliance"] = "not evaluated"
        else:
            metrics["relational_compliance"] = "n/a"

        return metrics

    import pandas as pd

    def detect_duplicates(df):
        return df.duplicated().sum()

    def check_null_columns(df):
        return df.isnull().sum().to_dict()

    def validate_column_types(df: pd.DataFrame, expected_schema: dict):
        mismatches = {}
        for col, expected_type in expected_schema.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if expected_type != actual_type:
                    mismatches[col] = {"expected": expected_type, "actual": actual_type}
        return mismatches

    def compute_field_quality_score(self, field, series, metrics):
        """
        Calcula un score de calidad (0 a 100) para el campo, penalizando desviaciones en:
          - Tipo de dato.
          - Valores nulos.
          - Outliers y sesgo.
          - Anomalías temporales.
          - Alta cardinalidad en campos categóricos.
          - Duplicados.
          - Falta de cumplimiento en seguridad.
        """
        score = 100
        if not metrics.get("type_match", False):
            score -= 20
        score -= metrics.get("null_percentage", 0) * 0.5
        if field.get("type") in ["integer", "float"]:
            outlier_pct = metrics.get("outlier_percentage", 0)
            score -= outlier_pct * 0.5
            skewness = metrics.get("skewness", 0)
            if skewness is not None and skewness > 1:
                score -= (skewness - 1) * 10
        if field.get("type") == "datetime":
            temporal_anomaly = metrics.get("temporal_anomaly", 0)
            if temporal_anomaly is not None and temporal_anomaly > 0:
                score -= temporal_anomaly * 10
        if field.get("type") == "string":
            cardinality_ratio = metrics.get("cardinality_ratio", 0)
            if cardinality_ratio is not None and cardinality_ratio > 0.8:
                score -= (cardinality_ratio - 0.8) * 50
        duplicate_pct = metrics.get("duplicate_percentage", 0)
        score -= duplicate_pct * 0.2
        if metrics.get("security_compliant") is False:
            score -= 15
        return max(score, 0)

    def generate_quality_metrics(self):
        """
        Genera un diccionario con métricas individuales por campo y métricas globales,
        integrando todas las validaciones (nulos, outliers, estadísticas, seguridad, etc.).
        """
        metrics_dict = {}
        field_scores = []

        for field in self.policy.get("fields", []):
            col_name = field.get("field_name")
            if col_name not in self.df.columns:
                metrics_dict[col_name] = {"status": "missing"}
                continue

            series = self.df[col_name]
            field_metrics = self.compute_field_metrics(field, series)
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
            drift = ((global_score - self.historical_global_score) / self.historical_global_score) * 100
            metrics_dict["global"]["drift_percentage"] = drift

        return metrics_dict
