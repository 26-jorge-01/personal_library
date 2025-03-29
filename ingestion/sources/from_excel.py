import pandas as pd
import logging
import os
from ingestion.base.dataset_loader import BaseDatasetLoader

class ExcelDatasetLoader(BaseDatasetLoader):
    """
    Implementación concreta para cargar datasets desde archivos Excel (.xlsx/.xls).
    Compatible con múltiples hojas, incluye logging y metadatos.
    """
    def __init__(self):
        super().__init__(source_name="excel")
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def load_data(self, file_path: str, sheet_name: str = 0, header: int = 0) -> pd.DataFrame:
        """
        Carga datos desde un archivo Excel.

        Args:
            file_path (str): Ruta del archivo Excel (.xlsx o .xls).
            sheet_name (str or int): Nombre o índice de la hoja a cargar (por defecto la primera).
            header (int): Fila que se usará como encabezado (por defecto 0).

        Returns:
            pd.DataFrame: Datos cargados en un DataFrame.
        """
        self.metadata["file_path"] = file_path
        self.metadata["sheet_name"] = sheet_name
        self.metadata["header_row"] = header

        try:
            self.logger.debug("Attempting to load Excel file from: %s, sheet: %s", file_path, sheet_name)

            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Excel file not found: {file_path}")

            df = pd.read_excel(file_path, sheet_name=sheet_name, header=header)

            self.metadata["row_count"] = len(df)
            self.metadata["columns"] = df.columns.tolist()
            self.metadata["status"] = "success"

            self.logger.info("Loaded %d rows from Excel: %s", len(df), file_path)
            return df

        except Exception as e:
            self.metadata["status"] = "failed"
            self.metadata["error"] = str(e)
            self.logger.error("Error loading Excel file: %s", e, exc_info=True)
            return pd.DataFrame()
