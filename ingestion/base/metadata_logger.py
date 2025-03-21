import pandas as pd
from datetime import datetime
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
        metadata["timestamp"] = datetime.now(datetime.timezone.utc).isoformat()
        self.records.append(metadata)
        self.logger.debug("Logged metadata: %s", metadata)

    def save(self) -> None:
        """
        Guarda todos los registros de metadatos en un archivo Parquet.
        """
        df = pd.DataFrame(self.records)
        df.to_parquet(self.report_path, index=False)
        self.logger.info("Metadata log saved to %s", self.report_path)
