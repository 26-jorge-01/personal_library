import os
import requests
import logging
from urllib.parse import urlencode

# Configuración básica del logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class GoogleMapsPlacesService:
    """
    Cliente para la API de Google Maps Places v2.

    Este servicio permite realizar:
      - Búsquedas por texto (Text Search)
      - Búsquedas cercanas (Nearby Search)
      - Obtener detalles de un lugar (Place Details)
      - Autocompletar (Place Autocomplete)
      - Generar URL para obtener fotos (Place Photo)

    La autenticación se realiza obligatoriamente mediante un API key.
    """
    def __init__(self, api_key):
        """
        Inicializa el servicio.

        :param api_key: API key de Google Maps Places.
        :raises ValueError: Si no se proporciona un API key.
        """
        if not api_key:
            raise ValueError("Debes proporcionar un API key para autenticarte con la API de Google Maps Places.")
        self.api_key = api_key
        self.base_url = "https://maps.googleapis.com/maps/api/place"

    def _request(self, endpoint, params):
        """
        Realiza una solicitud GET a la API de Places.

        :param endpoint: Endpoint de la API (ej. 'textsearch', 'nearbysearch', 'details', 'autocomplete', 'photo').
        :param params: Diccionario de parámetros de consulta.
        :return: En caso de endpoints JSON, retorna la respuesta JSON o None en caso de error.
                 Para el endpoint 'photo', retorna la URL construida.
        """
        if endpoint.lower() == "photo":
            return self._build_photo_url(params)
        
        url = f"{self.base_url}/{endpoint}/json"
        # Agregar el API key a los parámetros de consulta
        params['key'] = self.api_key

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            status = data.get("status")
            if status not in ("OK", "ZERO_RESULTS"):
                logger.error("Error en la respuesta de la API: %s - %s", status, data.get("error_message"))
                return None
            return data
        except requests.RequestException as e:
            logger.error("Error al realizar solicitud a %s: %s", url, e)
            return None

    def _build_photo_url(self, params):
        """
        Construye la URL para el endpoint de fotos. Este endpoint redirige a la imagen.
        """
        params['key'] = self.api_key
        url = f"{self.base_url}/photo?{urlencode(params)}"
        return url

    def text_search(self, query, **kwargs):
        """
        Realiza una búsqueda de lugares usando una consulta de texto.

        :param query: Consulta en formato texto.
        :param kwargs: Otros parámetros opcionales (ej. location, radius, type, language).
        :return: Diccionario con los resultados o None en caso de error.
        """
        params = {'query': query}
        params.update(kwargs)
        return self._request("textsearch", params)

    def nearby_search(self, location, radius, **kwargs):
        """
        Realiza una búsqueda de lugares cercanos.

        :param location: Ubicación en formato "lat,lng" (ej. "40.748817,-73.985428").
        :param radius: Radio en metros.
        :param kwargs: Otros parámetros opcionales (ej. keyword, type, opennow).
        :return: Diccionario con los resultados o None en caso de error.
        """
        params = {'location': location, 'radius': radius}
        params.update(kwargs)
        return self._request("nearbysearch", params)

    def place_details(self, place_id, **kwargs):
        """
        Obtiene detalles de un lugar usando su place_id.

        :param place_id: Identificador del lugar.
        :param kwargs: Otros parámetros opcionales (ej. fields, language).
        :return: Diccionario con los detalles o None en caso de error.
        """
        params = {'place_id': place_id}
        params.update(kwargs)
        return self._request("details", params)

    def place_autocomplete(self, input_text, **kwargs):
        """
        Realiza una consulta de autocompletar de lugares.

        :param input_text: Texto de entrada.
        :param kwargs: Otros parámetros opcionales (ej. location, radius, types, components, language).
        :return: Diccionario con las predicciones o None en caso de error.
        """
        params = {'input': input_text}
        params.update(kwargs)
        return self._request("autocomplete", params)

    def place_photo(self, photo_reference, max_width=None, max_height=None):
        """
        Genera la URL para obtener una foto de un lugar usando una referencia de foto.

        :param photo_reference: Referencia de la foto.
        :param max_width: Ancho máximo (opcional).
        :param max_height: Altura máxima (opcional).
        :return: URL construida para obtener la foto.
        """
        params = {'photoreference': photo_reference}
        if max_width:
            params['maxwidth'] = max_width
        if max_height:
            params['maxheight'] = max_height
        return self._request("photo", params)
    
    def get_reviews(self, place_id, language=None):
        """
        Obtiene las reseñas de un lugar dado su place_id.

        :param place_id: Identificador del lugar en Google Maps.
        :param language: (Opcional) Código de idioma para la respuesta (ej. "es" o "en").
        :return: Lista de reseñas o None si no se encuentran.
        """
        # Especificamos el campo 'reviews' para reducir el payload (además, se puede solicitar el nombre)
        details = self.place_details(place_id, language=language, fields="name,reviews")
        if details and details.get("result") and details["result"].get("reviews"):
            return details["result"]["reviews"]
        else:
            logger.info("No se encontraron reseñas para el place_id: %s", place_id)
            return None