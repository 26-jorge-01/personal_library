import json
import pandas as pd
from collections.abc import Mapping

class DataFlattener:
    def __init__(self,
                 separator='_',
                 max_depth=None,
                 flatten_collections=False,
                 parse_json=False,
                 convert_keys_to_str=True,
                 detect_cycles=True,
                 error_handling='raise'):
        """
        Inicializa el DataFlattener con opciones de parametrización.

        Parámetros:
          - separator: Caracter para concatenar claves (por defecto '_').
          - max_depth: Profundidad máxima para aplanar (None para ilimitado).
          - flatten_collections: Si True, aplanará diccionarios dentro de colecciones.
          - parse_json: Si True, intentará parsear cadenas JSON válidas.
          - convert_keys_to_str: Si True, convierte claves a string para asegurar nombres consistentes.
          - detect_cycles: Si True, detecta ciclos en la estructura para evitar bucles infinitos.
          - error_handling: Estrategia de manejo de errores ('raise', 'skip' o 'log').
        """
        self.separator = separator
        self.max_depth = max_depth
        self.flatten_collections = flatten_collections
        self.parse_json = parse_json
        self.convert_keys_to_str = convert_keys_to_str
        self.detect_cycles = detect_cycles
        self.error_handling = error_handling

    def flatten(self, data, parent_key='', current_depth=0, visited=None):
        """
        Aplana de forma recursiva una estructura anidada (usualmente diccionarios)
        concatenando los nombres de las claves en el formato 'nivel1_nivel2_campofinal'.

        Parámetros:
          - data: La estructura de datos a aplanar.
          - parent_key: Prefijo acumulado para la clave.
          - current_depth: Profundidad actual de la recursión.
          - visited: Conjunto de identificadores para la detección de ciclos.
        Retorna:
          - Un diccionario aplanado.
        """
        if visited is None:
            visited = set()
        items = {}

        # Detectar ciclos si está activado
        if self.detect_cycles and isinstance(data, dict):
            if id(data) in visited:
                if self.error_handling == 'raise':
                    raise ValueError("Ciclo detectado en la estructura de datos")
                elif self.error_handling == 'skip':
                    return {}
                return {}
            visited.add(id(data))

        # Si se alcanza la profundidad máxima, se asigna el valor tal cual
        if self.max_depth is not None and current_depth >= self.max_depth:
            items[parent_key] = data
            return items

        if isinstance(data, Mapping):
            for key, value in data.items():
                if self.convert_keys_to_str:
                    key = str(key)
                new_key = f"{parent_key}{self.separator}{key}" if parent_key else key

                # Intentar parsear cadenas JSON si se ha activado
                if self.parse_json and isinstance(value, str):
                    try:
                        parsed = json.loads(value)
                        value = parsed
                    except Exception:
                        pass

                if isinstance(value, Mapping):
                    items.update(self.flatten(value, new_key, current_depth + 1, visited=visited))
                elif isinstance(value, (list, tuple, set)) and not isinstance(value, (str, bytes)):
                    if self.flatten_collections:
                        for i, element in enumerate(value):
                            sub_key = f"{new_key}{self.separator}{i}"
                            if isinstance(element, Mapping):
                                items.update(self.flatten(element, sub_key, current_depth + 1, visited=visited))
                            else:
                                items[sub_key] = element
                    else:
                        items[new_key] = value
                else:
                    items[new_key] = value
        else:
            items[parent_key] = data
        return items

    def disaggregate(self, flat_dict):
        """
        Desagrega colecciones (listas, tuplas, sets) en elementos individuales,
        añadiendo un sufijo numérico a la clave original.

        Parámetros:
          - flat_dict: Diccionario aplanado.
        Retorna:
          - Un diccionario con las colecciones desagregadas.
        """
        result = {}
        for key, value in flat_dict.items():
            if isinstance(value, (list, tuple, set)) and not isinstance(value, (str, bytes)):
                for index, element in enumerate(value):
                    new_key = f"{key}{self.separator}{index}"
                    result[new_key] = element
            else:
                result[key] = value
        return result

    def process(self, data, disaggregate=True):
        """
        Procesa la entrada, que puede ser un diccionario, una lista de diccionarios o un DataFrame,
        aplicando el aplanamiento y, opcionalmente, la desagregación de colecciones, para retornar
        un DataFrame con la información extraída.

        Parámetros:
          - data: dict, lista de dicts o pandas DataFrame.
          - disaggregate: Si True, se aplicará la desagregación de colecciones.
        Retorna:
          - Un DataFrame de pandas con los datos procesados.
        """
        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient='records')
        elif isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            raise ValueError("Tipo de entrada no soportado. Se espera dict, list o DataFrame.")

        processed = []
        for record in data:
            try:
                flat = self.flatten(record)
                if disaggregate:
                    flat = self.disaggregate(flat)
                processed.append(flat)
            except Exception as e:
                if self.error_handling == 'raise':
                    raise e
                elif self.error_handling == 'skip':
                    continue
                # 'log' podría implementarse para registrar el error sin detener el proceso.
        return pd.DataFrame(processed)
