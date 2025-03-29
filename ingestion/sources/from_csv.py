import pandas as pd
import logging
import os
from ingestion.base.dataset_loader import BaseDatasetLoader

class CSVDatasetLoader(BaseDatasetLoader):
    """
    Implementación concreta para cargar datasets desde archivos CSV.
    Incorpora logging, trazabilidad y manejo de errores.
    """
    def __init__(self):
        super().__init__(source_name="csv")
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def load_data(self, file_path: str, delimiter: str = ",", encoding: str = "utf-8") -> pd.DataFrame:
        """
        Carga datos desde un archivo CSV local o remoto.

        Args:
            file_path (str): Ruta local o URL del archivo CSV.
            delimiter (str): Delimitador del archivo.
            encoding (str): Codificación del archivo.

        Returns:
            pd.DataFrame: Datos cargados en un DataFrame.
        """
        self.metadata["file_path"] = file_path
        self.metadata["delimiter"] = delimiter
        self.metadata["encoding"] = encoding

        try:
            self.logger.debug("Attempting to load CSV from: %s", file_path)

            if file_path.startswith("http"):
                df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
            elif os.path.exists(file_path):
                df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)
            else:
                raise FileNotFoundError(f"File not found: {file_path}")

            self.metadata["row_count"] = len(df)
            self.metadata["columns"] = df.columns.tolist()
            self.metadata["status"] = "success"

            self.logger.info("Loaded %d rows from CSV: %s", len(df), file_path)
            return df

        except Exception as e:
            self.metadata["status"] = "failed"
            self.metadata["error"] = str(e)
            self.logger.error("Error loading CSV file: %s", e, exc_info=True)
            return pd.DataFrame()
