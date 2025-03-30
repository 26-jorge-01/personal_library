import pandas as pd
import logging
import os
from ingestion.base.dataset_loader import BaseDatasetLoader

class ParquetDatasetLoader(BaseDatasetLoader):
    """
    Implementación concreta para cargar datasets desde archivos Parquet.
    Mantiene trazabilidad, logging y manejo de errores.
    """
    def __init__(self):
        super().__init__(source_name="parquet")
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def load_data(self, file_path: str) -> pd.DataFrame:
        """
        Carga datos desde un archivo Parquet.

        Args:
            file_path (str): Ruta local del archivo Parquet.

        Returns:
            pd.DataFrame: Datos cargados en un DataFrame.
        """
        self.metadata["file_path"] = file_path

        try:
            self.logger.debug("Attempting to load Parquet file from: %s", file_path)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Parquet file not found: {file_path}")

            df = pd.read_parquet(file_path)

            self.metadata["row_count"] = len(df)
            self.metadata["columns"] = df.columns.tolist()
            self.metadata["status"] = "success"

            self.logger.info("Loaded %d rows from Parquet: %s", len(df), file_path)
            return df

        except Exception as e:
            self.metadata["status"] = "failed"
            self.metadata["error"] = str(e)
            self.logger.error("Error loading Parquet file: %s", e, exc_info=True)
            return pd.DataFrame()
