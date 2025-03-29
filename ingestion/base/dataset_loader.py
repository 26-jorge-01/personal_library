from abc import ABC, abstractmethod
import pandas as pd
import logging

class BaseDatasetLoader(ABC):
    """
    Abstract Base Class para loaders de datasets.
    Define una interfaz común para extraer datos de diversas fuentes.
    """
    def __init__(self, source_name: str):
        self.source_name: str = source_name
        self.metadata: dict = {
            "source": source_name,
            "status": None,
            "error": None,
            "row_count": 0,
            "timestamp": None,
            "filters_applied": None,
        }

    @abstractmethod
    def load_data(self, **kwargs) -> pd.DataFrame:
        """
        Método abstracto para cargar datos.
        """
        pass

    def validate_input_schema(self, df: pd.DataFrame, policy: dict):
        """
        Valida que el DataFrame coincida con el esquema definido en la política.
        Devuelve diccionario con columnas que no coinciden por tipo.
        """
        try:
            expected_schema = {k: v.get("type") for k, v in policy.get("fields", {}).items()}
            mismatches = validate_column_types(df, expected_schema)
            if mismatches:
                logging.warning(f"Esquema no válido. Mismatches detectados: {mismatches}")
            else:
                logging.info("Esquema del dataset validado exitosamente contra la política.")
            return mismatches
        except Exception as e:
            logging.error(f"Error al validar el esquema de entrada: {e}")
            return {"error": str(e)}

def validate_column_types(df: pd.DataFrame, expected_schema: dict):
    mismatches = {}
    for col, expected_type in expected_schema.items():
        if col in df.columns:
            actual_type = str(df[col].dtype)
            if expected_type != actual_type:
                mismatches[col] = {"expected": expected_type, "actual": actual_type}
    return mismatches
