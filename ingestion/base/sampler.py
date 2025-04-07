import math
import random
import logging
import os
import pandas as pd

def get_z_value(confidence_level):
    """
    Retorna el valor crítico Z asociado al nivel de confianza.
    Soporta niveles comunes: 0.90, 0.95, 0.99.
    """
    if confidence_level == 0.90:
        return 1.645
    elif confidence_level == 0.95:
        return 1.96
    elif confidence_level == 0.99:
        return 2.576
    else:
        try:
            from scipy.stats import norm
            return norm.ppf(1 - (1 - confidence_level) / 2)
        except ImportError:
            raise ValueError("Nivel de confianza no soportado sin scipy. Pruebe con 0.90, 0.95 o 0.99.")

def calculate_sample_size(population_total, confidence_level=0.95, margin_error=0.05, p=0.5):
    """
    Calcula el tamaño de la muestra para estimar una proporción en una población.
    
    Parámetros:
      - population_total: Total de la población.
      - confidence_level: Nivel de confianza deseado (por defecto 0.95).
      - margin_error: Margen de error permitido (por defecto 0.05).
      - p: Proporción estimada (por defecto 0.5).
      
    Retorna:
      - Tamaño de muestra (entero).
    """
    if population_total <= 0:
        raise ValueError("El total de la población debe ser mayor a cero.")
    z = get_z_value(confidence_level)
    n_0 = (z**2 * p * (1 - p)) / (margin_error**2)
    n = n_0 / (1 + ((n_0 - 1) / population_total))
    logging.info("Cálculo de tamaño de muestra: población=%d, nivel_confianza=%.2f, margen_error=%.2f, p=%.2f => n=%d",
                 population_total, confidence_level, margin_error, p, math.ceil(n))
    return math.ceil(n)

class Sampler:
    def __init__(self, data, population_total=None, config=None):
        """
        Inicializa el muestreador.
        
        Parámetros:
          - data: Lista, tupla o DataFrame de pandas con los datos entrantes.
          - population_total: Total de la población. Si es None, se usa len(data) o data.shape[0] en caso de DataFrame.
          - config: Diccionario opcional para la configuración (nivel de confianza, margen de error, p).
                    Se pueden sobreescribir por variables de entorno:
                      * CONFIDENCE_LEVEL
                      * MARGIN_ERROR
                      * ESTIMATED_P
        """
        # Verificar que la data sea de un tipo soportado
        if not isinstance(data, (list, tuple, pd.DataFrame)):
            raise ValueError("La data debe ser una lista, tupla o un DataFrame de pandas.")
        self.data = data
        
        if isinstance(data, pd.DataFrame):
            self.population_total = population_total if population_total is not None else data.shape[0]
        else:
            self.population_total = population_total if population_total is not None else len(data)
        
        # Configuración por defecto
        default_config = {
            'confidence_level': float(os.getenv('CONFIDENCE_LEVEL', 0.95)),
            'margin_error': float(os.getenv('MARGIN_ERROR', 0.05)),
            'p': float(os.getenv('ESTIMATED_P', 0.5))
        }
        if config:
            default_config.update(config)
        self.config = default_config
        self.logger = logging.getLogger(__name__)
        self.logger.info("Sampler inicializado con población_total=%d y configuración=%s", 
                     self.population_total, self.config)
    
    def get_sample_size(self):
        """
        Calcula y retorna el tamaño de muestra basado en la configuración y población definida.
        """
        return calculate_sample_size(
            self.population_total,
            confidence_level=self.config['confidence_level'],
            margin_error=self.config['margin_error'],
            p=self.config['p']
        )
    
    def perform_sampling(self, sample_size=None, seed=None):
        """
        Realiza un muestreo aleatorio simple sin reemplazo.
        
        Parámetros:
          - sample_size: Tamaño de la muestra. Si es None, se calcula a partir de la configuración.
          - seed: Semilla para la reproducibilidad.
          
        Retorna:
          - Una muestra de los datos en el mismo formato de la entrada (lista o DataFrame).
        """
        if seed is not None:
            random.seed(seed)
            self.logger.info("Semilla establecida para muestreo: %s", seed)
        
        if sample_size is None:
            sample_size = self.get_sample_size()
        sample_size = min(sample_size, self.population_total)
        self.logger.info("Realizando muestreo: tamaño de muestra=%d de un total de %d registros", sample_size, self.population_total)
        
        # Si los datos son un DataFrame
        if isinstance(self.data, pd.DataFrame):
            return self.data.sample(n=sample_size, random_state=seed)
        else:
            return random.sample(self.data, sample_size)
    
    def perform_stratified_sampling(self, stratifier, strata_sample_sizes, seed=None):
        """
        Realiza un muestreo estratificado basado en una función que determine el estrato de cada dato.
        
        Parámetros:
          - stratifier: Función o nombre de columna (en caso de DataFrame) que recibe un dato y retorna el estrato al que pertenece.
          - strata_sample_sizes: Diccionario con el tamaño de muestra deseado para cada estrato.
          - seed: Semilla para la reproducibilidad.
          
        Retorna:
          - Una muestra estratificada en el mismo formato que la data de entrada (lista o DataFrame).
        """
        if seed is not None:
            random.seed(seed)
            self.logger.info("Semilla establecida para muestreo estratificado: %s", seed)
        
        # Si la data es un DataFrame y stratifier es una columna existente
        if isinstance(self.data, pd.DataFrame) and isinstance(stratifier, str) and stratifier in self.data.columns:
            stratified_sample = []
            grouped = self.data.groupby(stratifier)
            for key, group in grouped:
                n = strata_sample_sizes.get(key, group.shape[0])
                n = min(n, group.shape[0])
                self.logger.info("Muestreo para estrato '%s': tamaño de muestra=%d de %d registros", key, n, group.shape[0])
                stratified_sample.append(group.sample(n=n, random_state=seed))
            return pd.concat(stratified_sample)
        else:
            # Manejo para listas o para DataFrame cuando stratifier es una función
            strata = {}
            if isinstance(self.data, pd.DataFrame):
                # Convertir DataFrame a lista de registros (diccionarios)
                data_iter = self.data.to_dict(orient='records')
            else:
                data_iter = self.data
            for item in data_iter:
                key = stratifier(item) if callable(stratifier) else item.get(stratifier)
                strata.setdefault(key, []).append(item)
            
            sampled_data = []
            for key, items in strata.items():
                n = strata_sample_sizes.get(key, len(items))
                n = min(n, len(items))
                self.logger.info("Muestreo para estrato '%s': tamaño de muestra=%d de %d registros", key, n, len(items))
                sampled_data.extend(random.sample(items, n))
            # Si la entrada original era un DataFrame, reconvertir la lista de registros a DataFrame
            if isinstance(self.data, pd.DataFrame):
                return pd.DataFrame(sampled_data)
            else:
                return sampled_data