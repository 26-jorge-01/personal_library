import pandas as pd
from datetime import datetime, timezone
import uuid
import os
import logging

class MetadataLogger:
    """
    Logger para auditar y almacenar metadatos de cada proceso de ingesta.
    Permite trazar cada ejecución con un ID único, timestamp y estado.
    """
    def __init__(self, report_path: str = "reports/audit_log.parquet"):
        self.records = []
        self.report_path = report_path
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def log(self, metadata: dict) -> None:
        """
        Registra metadatos de un proceso de ingesta.
        """
        metadata["uuid"] = str(uuid.uuid4())
        metadata["timestamp"] = datetime.now(timezone.utc).isoformat()
        self.records.append(metadata)
        self.logger.debug("Logged metadata: %s", metadata)

    def log_error(self, error_msg: str, context: dict = None) -> None:
        """
        Registra un error crítico con contexto opcional.
        """
        metadata = {
            "uuid": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "error",
            "message": error_msg,
            "context": context or {}
        }
        self.records.append(metadata)
        self.logger.error("Error registrado: %s", metadata)

    def save(self) -> None:
        """
        Guarda todos los registros de metadatos en un archivo Parquet.
        Elimina duplicados por UUID antes de guardar.
        """
        df = pd.DataFrame(self.records)
        if os.path.exists(self.report_path):
            try:
                existing_df = pd.read_parquet(self.report_path)
                df = pd.concat([existing_df, df], ignore_index=True)
            except Exception as e:
                self.logger.warning("No se pudo leer el archivo existente: %s", e)
        df = df.drop_duplicates(subset="uuid")
        df.to_parquet(self.report_path, index=False, engine="pyarrow")
        self.logger.info("Metadata log saved to %s", self.report_path)

    def load(self) -> pd.DataFrame:
        """
        Carga registros previos del archivo de log.
        """
        if os.path.exists(self.report_path):
            return pd.read_parquet(self.report_path)
        else:
            self.logger.warning("Archivo de log no encontrado: %s", self.report_path)
            return pd.DataFrame()
