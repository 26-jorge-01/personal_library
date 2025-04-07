"""
Module: metadata_logger.py

Descripción:
    Este módulo gestiona la auditoría y el almacenamiento de metadatos generados en procesos
    de ingesta, transformación y validación de datos. Permite registrar logs con información
    adicional (UUID, timestamp, estado, etc.) y guardarlos en múltiples formatos (Parquet, CSV o JSON)
    según la configuración establecida.

Funcionalidades:
    - log(metadata: dict): Registra un evento o acción, añadiendo un identificador único y marca temporal.
    - log_error(error_msg: str, context: dict = None): Registra un error crítico, agregando detalles y contexto.
    - save(): Guarda todos los registros acumulados en el archivo configurado, en el formato deseado.
    - load() -> pd.DataFrame: Carga y retorna los registros previamente guardados en un DataFrame.

Uso:
    Desde otro script o notebook, se puede importar la clase MetadataLogger y utilizarla para gestionar
    la auditoría de procesos, configurando el formato de salida y la ruta del archivo de logs según se requiera.

Ejemplo:
    >>> from metadata_logger import MetadataLogger
    >>> ml = MetadataLogger(report_path="reports/audit_log.csv", file_format="csv")
    >>> ml.log({"proceso": "ingesta", "detalle": "Inicio de ingesta de datos"})
    >>> ml.log_error("Fallo en la validación", {"columna": "email"})
    >>> ml.save()
    >>> df_logs = ml.load()
    >>> print(df_logs)
"""

import pandas as pd
from datetime import datetime, timezone
import uuid
import os
import logging
import json

class MetadataLogger:
    """
    Logger para auditar y almacenar metadatos de procesos de ingesta y transformación de datos.

    Permite:
      - Registrar eventos (éxitos o errores) con un identificador único y marca temporal.
      - Guardar los registros en formatos soportados: 'parquet', 'csv' o 'json'.
      - Cargar registros previos desde el archivo configurado.

    Parámetros de inicialización:
      - report_path (str): Ruta del archivo donde se guardarán los registros.
      - file_format (str): Formato de guardado ('parquet', 'csv' o 'json'). Si no se especifica,
                           se infiere de la extensión de report_path (default: 'parquet').
    """

    SUPPORTED_FORMATS = ['parquet', 'csv', 'json']
    
    def __init__(self, report_path: str = "reports/audit_log.parquet", file_format: str = None):
        self.records = []
        self.report_path = report_path
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        # Determinar el formato de salida
        if file_format is None:
            ext = os.path.splitext(report_path)[1].lower().replace('.', '')
            self.file_format = ext if ext in self.SUPPORTED_FORMATS else 'parquet'
        else:
            if file_format.lower() not in self.SUPPORTED_FORMATS:
                raise ValueError(f"Formato no soportado. Los formatos soportados son: {self.SUPPORTED_FORMATS}")
            self.file_format = file_format.lower()
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("MetadataLogger inicializado con report_path='%s' y file_format='%s'", 
                          self.report_path, self.file_format)

    def log(self, metadata: dict) -> None:
        """
        Registra metadatos de un proceso de ingesta o transformación.

        Parámetros:
          - metadata (dict): Diccionario con la información a registrar. Se añadirá automáticamente:
              - 'uuid': Identificador único generado.
              - 'timestamp': Marca temporal en formato ISO.
              - 'status': Estado del registro (por defecto 'ok', si no se especifica).
        """
        metadata["uuid"] = str(uuid.uuid4())
        metadata["timestamp"] = datetime.now(timezone.utc).isoformat()
        metadata.setdefault("status", "ok")
        self.records.append(metadata)
        self.logger.debug("Logged metadata: %s", metadata)

    def log_error(self, error_msg: str, context: dict = None) -> None:
        """
        Registra un error crítico, incluyendo un mensaje y contexto opcional.

        Parámetros:
          - error_msg (str): Mensaje descriptivo del error.
          - context (dict, opcional): Información adicional o contexto del error.
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
        Guarda todos los registros acumulados en el archivo configurado.
        
        - Se verifica si existe un archivo previo, en cuyo caso se carga y se concatenan los registros.
        - Se eliminan duplicados basados en el 'uuid'.
        - El DataFrame resultante se guarda en el formato especificado ('parquet', 'csv' o 'json').
        """
        df = pd.DataFrame(self.records)
        # Si existe un archivo previo, intenta cargarlo y concatenar registros
        if os.path.exists(self.report_path):
            try:
                existing_df = self.load()
                df = pd.concat([existing_df, df], ignore_index=True)
            except Exception as e:
                self.logger.warning("No se pudo leer el archivo existente: %s", e)
        df = df.drop_duplicates(subset="uuid")
        
        try:
            if self.file_format == 'parquet':
                df.to_parquet(self.report_path, index=False, engine="pyarrow")
            elif self.file_format == 'csv':
                df.to_csv(self.report_path, index=False)
            elif self.file_format == 'json':
                df.to_json(self.report_path, orient="records", lines=True)
            else:
                raise ValueError(f"Formato no soportado: {self.file_format}")
            self.logger.info("Metadata log guardado en %s con formato %s", self.report_path, self.file_format)
        except Exception as e:
            self.logger.error("Error al guardar el log en %s: %s", self.report_path, e)

    def load(self) -> pd.DataFrame:
        """
        Carga los registros previos desde el archivo de log y los retorna como un DataFrame.

        Retorna:
          - DataFrame con los registros almacenados. Si ocurre un error o no se encuentra el archivo,
            se retorna un DataFrame vacío.
        """
        if os.path.exists(self.report_path):
            try:
                if self.file_format == 'parquet':
                    return pd.read_parquet(self.report_path)
                elif self.file_format == 'csv':
                    return pd.read_csv(self.report_path)
                elif self.file_format == 'json':
                    return pd.read_json(self.report_path, orient="records", lines=True)
                else:
                    raise ValueError(f"Formato no soportado: {self.file_format}")
            except Exception as e:
                self.logger.error("Error al cargar el archivo de log: %s", e)
                return pd.DataFrame()
        else:
            self.logger.warning("Archivo de log no encontrado: %s", self.report_path)
            return pd.DataFrame()