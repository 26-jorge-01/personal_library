from abc import ABC, abstractmethod
import pandas as pd

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
