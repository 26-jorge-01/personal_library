import logging

# Configuración global de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ===================================================
# Registro Centralizado de Métricas (MetricRegistry)
# ===================================================
class MetricRegistry:
    def __init__(self):
        # Se almacena un diccionario para métricas a nivel de campo y pipeline.
        self.field_metrics = {}        # Ejemplo: { "word_count": {"func": func, "phases": ["ingestion", "cleaning"] } }
        self.pipeline_metrics = {}

    def register_field_metric(self, name: str, func: callable, phases: list = None) -> None:
        """
        Registra una métrica a nivel de campo.
        :param name: Nombre de la métrica.
        :param func: Función que calcula la métrica; recibe (field, series, current_metrics).
        :param phases: Lista de fases en las que se debe aplicar (por defecto, todas).
        """
        self.field_metrics[name] = {"func": func, "phases": phases or ["ingestion", "cleaning", "indexing", "continuous_monitoring"]}
        logging.info(f"Métrica de campo '{name}' registrada para fases: {self.field_metrics[name]['phases']}.")

    def register_pipeline_metric(self, name: str, func: callable, phases: list = None) -> None:
        """
        Registra una métrica a nivel de pipeline.
        :param name: Nombre de la métrica.
        :param func: Función que calcula la métrica; recibe (monitor, report).
        :param phases: Lista de fases en las que se debe aplicar (por defecto, solo en reporte).
        """
        self.pipeline_metrics[name] = {"func": func, "phases": phases or ["report"]}
        logging.info(f"Métrica de pipeline '{name}' registrada para fases: {self.pipeline_metrics[name]['phases']}.")

    def get_field_metrics(self) -> dict:
        return self.field_metrics

    def get_pipeline_metrics(self) -> dict:
        return self.pipeline_metrics