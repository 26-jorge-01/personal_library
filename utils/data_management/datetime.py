import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def parse_to_timestamp(x):
    """
    Convierte un elemento x a pd.Timestamp de forma robusta.
    
    Soporta:
      - Objetos datetime o np.datetime64.
      - Diccionarios con claves (en cualquier orden o mayúsculas/minúsculas) que contengan al menos 'year', 'month' y 'day'. 
        También se consideran opcionales 'hour', 'minute', 'second' y 'microsecond'.
      - Listas o tuplas con al menos 3 elementos (year, month, day) y opcionalmente (hour, minute, second, microsecond).
      - Números (int o float) interpretados como timestamps en segundos.
      - Cadenas de texto u otros formatos reconocibles por pd.to_datetime.
    
    Si no se puede convertir, devuelve pd.NaT.
    """
    try:
        # Si ya es un objeto datetime, lo retornamos como Timestamp
        if isinstance(x, (pd.Timestamp, np.datetime64)):
            return pd.to_datetime(x)
        
        # Si es un diccionario, normalizamos las claves a minúsculas
        if isinstance(x, dict):
            x_norm = {str(k).lower(): v for k, v in x.items()}
            required = ['year', 'month', 'day']
            if all(key in x_norm for key in required):
                # Construir los argumentos obligatorios
                kwargs = {key: int(x_norm[key]) for key in required}
                # Agregar opcionales si existen
                for key in ['hour', 'minute', 'second', 'microsecond']:
                    if key in x_norm:
                        try:
                            kwargs[key] = int(x_norm[key])
                        except Exception as e:
                            # En caso de fallo en la conversión de algún opcional, se omite
                            logger.warning("No se pudo convertir la clave %s en %s: %s", key, x_norm[key], e)
                try:
                    return pd.Timestamp(**kwargs)
                except Exception as e:
                    logger.error("Error al convertir diccionario %s: %s", x, e)
                    return pd.NaT
            else:
                logger.warning("Diccionario con claves insuficientes: %s", x)
                return pd.NaT
        
        # Si es una lista o tupla con al menos 3 elementos, asumir (year, month, day) y opcionalmente hora, minuto, segundo, microsegundo
        if isinstance(x, (list, tuple)) and len(x) >= 3:
            try:
                year, month, day = int(x[0]), int(x[1]), int(x[2])
                kwargs = {'year': year, 'month': month, 'day': day}
                if len(x) >= 6:
                    kwargs['hour'] = int(x[3])
                    kwargs['minute'] = int(x[4])
                    kwargs['second'] = int(x[5])
                if len(x) >= 7:
                    kwargs['microsecond'] = int(x[6])
                return pd.Timestamp(**kwargs)
            except Exception as e:
                logger.error("Error al convertir lista/tupla %s: %s", x, e)
                return pd.NaT
        
        # Si es un número, intentar interpretarlo como timestamp en segundos.
        if isinstance(x, (int, float)):
            try:
                return pd.to_datetime(x, unit='s', errors='coerce')
            except Exception as e:
                logger.error("Error al convertir número %s: %s", x, e)
                return pd.NaT
        
        # Para otros tipos (por ejemplo, cadenas) se intenta la conversión normal.
        return pd.to_datetime(x, errors='coerce')
    
    except Exception as e:
        logger.error("Error en parse_to_timestamp para el valor %s: %s", x, e)
        return pd.NaT
