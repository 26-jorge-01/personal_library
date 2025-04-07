import pandas as pd
import logging
from governance.quality_management.data_remediation.imputation.rules.string import impute_empty_string
from governance.quality_management.data_remediation.normalization.rules.string import clean_special_characters

logger = logging.getLogger(__name__)

class AdvancedDataRemediationEngine:
    def __init__(self, df: pd.DataFrame, quality_report: dict):
        self.df = df.copy()
        self.quality_report = quality_report
        self.remediation_log = {}
        self.remediation_knowledge = {}

    def remediate_column(self, col: str):
        logger.info("Remediando columna: %s", col)
        series = self.df[col]
        actions = []
        col_report = self.quality_report.get(col, {})
        inferred_type = col_report.get("inferred_type", "unknown")
        actions.append(f"Tipo según reporte: {inferred_type}")

        if inferred_type in ["integer", "float"]:
            try:
                series = pd.to_numeric(series, errors='coerce')
                actions.append("Convertido a numérico")
            except Exception as e:
                actions.append(f"Error al convertir a numérico: {str(e)}")

        elif inferred_type == "datetime":
            try:
                series = pd.to_datetime(series, errors='coerce')
                actions.append("Convertido a datetime")
            except Exception as e:
                actions.append(f"Error al convertir a datetime: {str(e)}")

        elif inferred_type == "boolean":
            try:
                series = series.astype(bool)
                actions.append("Convertido a boolean")
            except Exception as e:
                actions.append(f"Error al convertir a boolean: {str(e)}")

        elif inferred_type == "string":
            try:
                series = series.astype(str)
                actions.append("Convertido a string")
            except Exception as e:
                actions.append(f"Error al convertir a string: {str(e)}")
            if series.isnull().any():
                # Imputar valores nulos con cadena vacía
                series, act = impute_empty_string(series)
                actions.append(act)
                # Limpiar caracteres especiales
                series, act = clean_special_characters(series)
                actions.append(act)
        else:
            actions.append("Tipo desconocido; no se aplicaron transformaciones específicas")

        self.df[col] = series
        self.remediation_log[col] = actions
        self.remediation_knowledge[col] = {
            "inferred_type": inferred_type,
            "actions": actions,
            "basic_metrics": col_report.get("basic_metrics", {}),
            "specific_metrics": col_report.get("specific_metrics", {})
        }
        logger.info("Acciones para %s: %s", col, actions)

    def remediate_all(self):
        logger.info("Iniciando remediación de todas las columnas")
        for col in self.df.columns:
            if col in self.quality_report:
                self.remediate_column(col)
            else:
                logger.warning("No se encontró quality_report para la columna %s; se omite.", col)
        logger.info("Remediación completada")
        return self.df, self.remediation_log