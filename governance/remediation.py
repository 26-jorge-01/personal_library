import os
import base64
import pandas as pd
from governance.automation_and_monitoring.automated_policies.engine_policy_autogen import get_or_create_policy, infer_column_type

def apply_encryption(value):
    """
    Función auxiliar para 'encriptar' el valor.
    Aquí se utiliza base64 como marcador de posición.
    En producción se debería utilizar un mecanismo de encriptación robusto.
    """
    try:
        if pd.isnull(value):
            return value
        # Convertir el valor a cadena
        value_str = str(value)
        encrypted = base64.b64encode(value_str.encode("utf-8")).decode("utf-8")
        return encrypted
    except Exception as e:
        return value

class RemediationEngine:
    def __init__(self, df: pd.DataFrame, policy_filename: str):
        """
        Inicializa el RemediationEngine con el dataset y la política correspondiente.
        
        :param df: DataFrame con los datos a remediar.
        :param policy_filename: Nombre del archivo de política (ej. "s2_contracts.yaml").
        """
        self.df = df.copy()
        self.policy = get_or_create_policy(df, policy_filename)
        self.remediation_log = {}  # Registro de acciones aplicadas por campo

    def remediate_field(self, field_policy):
        """
        Aplica acciones de remediación sobre una columna según la política definida.
        
        Las acciones incluyen:
          - Conversión forzada al tipo de dato esperado.
          - Imputación de valores nulos.
          - Tratamiento de outliers (winsorización).
          - Aplicación de medidas de seguridad: enmascaramiento o encriptación.
        
        Actualiza la columna en el DataFrame y registra las acciones realizadas.
        """
        col_name = field_policy.get("field_name")
        if col_name not in self.df.columns:
            self.remediation_log[col_name] = "Field missing: cannot remediate."
            return

        series = self.df[col_name]
        expected_type = field_policy.get("type")
        actions = []

        # 1. Forzar conversión de tipo
        try:
            if expected_type == "integer":
                series = pd.to_numeric(series, errors="coerce").astype("Int64")
                actions.append("converted to integer")
            elif expected_type == "float":
                series = pd.to_numeric(series, errors="coerce").astype("float")
                actions.append("converted to float")
            elif expected_type == "datetime":
                series = pd.to_datetime(series, errors="coerce")
                actions.append("converted to datetime")
            elif expected_type == "boolean":
                series = series.astype("bool")
                actions.append("converted to boolean")
            # Si es string, se deja tal cual.
        except Exception as e:
            actions.append(f"conversion error: {e}")

        # 2. Imputación de valores nulos
        if "no_nulls" in field_policy.get("rules", []):
            if series.isnull().any():
                if expected_type in ["integer", "float"]:
                    median_value = series.median()
                    series.fillna(median_value, inplace=True)
                    actions.append("imputed nulls with median")
                elif expected_type == "datetime":
                    mode_date = series.mode().iloc[0] if not series.mode().empty else pd.Timestamp('1970-01-01')
                    series.fillna(mode_date, inplace=True)
                    actions.append("imputed nulls with mode date")
                elif expected_type == "boolean":
                    series.fillna(False, inplace=True)
                    actions.append("imputed nulls with False")
                else:
                    series.fillna("", inplace=True)
                    actions.append("imputed nulls with empty string")

        # 3. Tratamiento de outliers en columnas numéricas mediante winsorización
        if expected_type in ["integer", "float"]:
            if series.notnull().all() and series.dtype.kind in "fi":
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                if expected_type == "integer":
                    lower_bound = int(round(lower_bound))
                    upper_bound = int(round(upper_bound))
                series = series.clip(lower=lower_bound, upper=upper_bound)
                actions.append("winsorized outliers")

        # 4. Aplicar medidas de seguridad según la política
        security_measure = field_policy.get("security")
        if security_measure == "masked":
            if expected_type == "string":
                series = series.apply(lambda x: x[:2] + "***" if isinstance(x, str) and len(x) > 3 else x)
                actions.append("masked sensitive data")
        elif security_measure == "encrypted":
            # Aplica encriptación a cada valor
            series = series.apply(lambda x: apply_encryption(x))
            actions.append("encrypted sensitive data")

        # Actualiza la columna y registra las acciones realizadas
        self.df[col_name] = series
        self.remediation_log[col_name] = actions

    def fill_missing(df, field, method="mode"):
        if method == "mean":
            value = df[field].mean()
        elif method == "mode":
            value = df[field].mode()[0]
        else:
            raise ValueError("Método de imputación no soportado")
        df[field] = df[field].fillna(value)
        return df

    def coerce_column_type(df, field, expected_type):
        try:
            df[field] = df[field].astype(expected_type)
        except Exception as e:
            raise ValueError(f"Error al convertir columna '{field}' a {expected_type}: {e}")
        return df

    def run_remediation(self):
        """
        Ejecuta la remediación para cada campo definido en la política.
        
        :return: DataFrame remediado.
        """
        for field in self.policy.get("fields", []):
            self.remediate_field(field)
        return self.df
