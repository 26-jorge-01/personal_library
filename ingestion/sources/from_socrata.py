import pandas as pd
import time
import logging
from requests.exceptions import Timeout
from ingestion.base.dataset_loader import BaseDatasetLoader

class SocrataDatasetLoader(BaseDatasetLoader):
    """
    Implementación concreta para cargar datasets desde Socrata.
    Incorpora un mecanismo robusto de reintentos y logging.
    """
    def __init__(self, client):
        super().__init__(source_name="socrata")
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def load_data(self, dataset_code: str, filters: dict = None, limit: int = 1000000) -> pd.DataFrame:
        """
        Carga datos de un dataset en Socrata.

        Args:
            dataset_code (str): Identificador del dataset en Socrata.
            filters (dict): Diccionario con filtros en formato {campo: (operador, valor)}.
            limit (int): Límite de registros a recuperar.

        Returns:
            pd.DataFrame: Datos cargados en un DataFrame.
        """
        self.metadata["dataset_code"] = dataset_code
        self.metadata["filters_applied"] = filters
        query = self._build_query(filters, limit)
        attempt = 0
        retries = 10
        delay = 2

        while attempt < retries:
            try:
                self.logger.debug("Executing query: %s", query)
                rows = self.client.get(dataset_code, content_type="json", query=query)
                df = pd.DataFrame.from_dict(rows)
                self.metadata["row_count"] = len(df)
                self.metadata["status"] = "success"
                self.logger.info("Loaded %d rows from dataset %s", len(df), dataset_code)
                return df
            except Timeout as e:
                attempt += 1
                self.logger.warning("Timeout attempt %d/%d for dataset %s: %s", attempt, retries, dataset_code, e)
                time.sleep(delay)
            except Exception as e:
                self.metadata["status"] = "failed"
                self.metadata["error"] = str(e)
                self.logger.error("Error loading dataset %s: %s", dataset_code, e, exc_info=True)
                break

        self.metadata["status"] = "failed"
        self.metadata["error"] = "Exceeded retry limit"
        self.logger.error("Failed to load dataset %s after %d attempts", dataset_code, retries)
        return pd.DataFrame()

    def _build_query(self, filters: dict, limit: int) -> str:
        """
        Construye la consulta Socrata a partir de los filtros y el límite.
        """
        if not filters:
            return f"SELECT * LIMIT {limit}"
        conditions = []
        for field, (op, value) in filters.items():
            if isinstance(value, str):
                value = f"'{value}'"
            conditions.append(f"{field} {op} {value}")
        where_clause = " AND ".join(conditions)
        return f"SELECT * WHERE {where_clause} LIMIT {limit}"
