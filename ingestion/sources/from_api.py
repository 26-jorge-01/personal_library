import pandas as pd
import requests
import logging
from typing import Union
from ingestion.base.dataset_loader import BaseDatasetLoader
from utils.data_flattener import DataFlattener

class APIDatasetLoader(BaseDatasetLoader):
    """
    Loader altamente flexible para extraer datos desde APIs REST (GET, POST...).
    Soporta autenticación, headers, paginación, cuerpos personalizados y extracción anidada.
    """
    def __init__(self):
        super().__init__(source_name="api")
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

    def load_data(
        self,
        url: str,
        method: str = "GET",
        headers: dict = None,
        params: dict = None,
        body: Union[dict, str] = None,
        body_type: str = "json",  # json, form, raw
        json_path: str = None,
        timeout: int = 15,
        pagination: dict = None,
        max_pages: int = 10,
        disaggregate: bool = False
    ) -> pd.DataFrame:
        """
        Carga datos desde una API REST.

        Args:
            url (str): Endpoint base.
            method (str): Método HTTP (GET, POST, etc.).
            headers (dict): Headers HTTP (tokens, content-type...).
            params (dict): Parámetros de la URL.
            body (dict or str): Cuerpo de la solicitud (para POST/PUT).
            body_type (str): Tipo de cuerpo: json, form, raw.
            json_path (str): Ruta al objeto de interés (ej: 'data.items').
            timeout (int): Tiempo máximo de espera.
            pagination (dict): Config dict con paginación (ej: {"param": "offset", "step": 100}).
            max_pages (int): Límite de iteraciones para paginación.
            disaggregate (bool): Si True, aplana la estructura anidada.

        Returns:
            pd.DataFrame: Dataset combinado de todas las páginas.
        """
        self.metadata.update({
            "url": url,
            "method": method,
            "headers_used": list(headers.keys()) if headers else None,
            "params": params,
            "body_type": body_type,
            "pagination": pagination
        })

        results = []
        current_page = 0
        while current_page < max_pages:
            try:
                req_params = params.copy() if params else {}

                # aplicar paginación
                if pagination:
                    param_name = pagination.get("param", "offset")
                    step = pagination.get("step", 100)
                    req_params[param_name] = current_page * step

                self.logger.debug("Requesting %s with params: %s", url, req_params)

                request_kwargs = {
                    "headers": headers,
                    "timeout": timeout,
                    "params": req_params
                }

                if method.upper() in ["POST", "PUT"]:
                    if body_type == "json":
                        request_kwargs["json"] = body
                    elif body_type == "form":
                        request_kwargs["data"] = body
                    elif body_type == "raw":
                        request_kwargs["data"] = str(body)

                response = requests.request(method.upper(), url, **request_kwargs)
                response.raise_for_status()
                data = response.json()

                # ir al nodo anidado si se especifica
                if json_path:
                    for key in json_path.split("."):
                        data = data[key]

                if isinstance(data, list):
                    results.extend(data)
                elif isinstance(data, dict):
                    results.append(data)
                else:
                    raise ValueError("Respuesta no procesable: debe ser lista o dict")

                if not pagination:
                    break

                # parar si no hay más resultados
                if pagination.get("stop_if_empty") and not data:
                    break

                current_page += 1

            except Exception as e:
                self.metadata["status"] = "failed"
                self.metadata["error"] = str(e)
                self.logger.error("Error en petición API: %s", e, exc_info=True)
                break

        # Procesar la estructura anidada usando DataFlattener para extraer el máximo de información
        flattener = DataFlattener(
            separator='_',
            max_depth=None,
            flatten_collections=True,
            parse_json=True,
            convert_keys_to_str=True,
            detect_cycles=True,
            error_handling='raise'
        )

        df = flattener.process(results, disaggregate)
        self.metadata["row_count"] = len(df)
        self.metadata["columns"] = df.columns.tolist()
        self.metadata["status"] = "success" if len(df) > 0 else "empty"
        self.logger.info("API: cargadas %d filas desde %s", len(df), url)

        return df
