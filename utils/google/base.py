import os
import pickle
import logging

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Configuración del logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class GoogleServiceBase:
    """
    Clase base para la integración con cualquier API de Google.
    
    Se encarga de autenticar al usuario y de construir el cliente de servicio.
    
    Parámetros:
      - service_name: Nombre del servicio (por ejemplo, 'gmail').
      - version: Versión del API (por ejemplo, 'v1').
      - scopes: Lista de scopes de acceso requeridos.
      - credentials_file: Archivo JSON de credenciales descargado de Google Cloud Console.
      - token_file: Archivo donde se guardarán los tokens de acceso para evitar autenticaciones repetidas.
    """
    
    def __init__(self, service_name, version, scopes,
                 credentials_file='credentials.json', token_file='token.pickle'):
        self.service_name = service_name
        self.version = version
        self.scopes = scopes
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.creds = None
        self.service = None
        self.authenticate()

    def authenticate(self):
        """
        Autentica al usuario utilizando OAuth 2.0.
        
        Carga las credenciales guardadas en token_file si existen y son válidas;
        de lo contrario, inicia el flujo de autenticación.
        """
        # Cargar credenciales guardadas si existen
        if os.path.exists(self.token_file):
            with open(self.token_file, 'rb') as token:
                self.creds = pickle.load(token)
        
        # Si las credenciales no existen o no son válidas, ejecutar el flujo de autenticación
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                try:
                    self.creds.refresh(Request())
                    logger.info("Credenciales refrescadas correctamente.")
                except Exception as e:
                    logger.error(f"Error al refrescar credenciales: {e}")
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                self.creds = flow.run_local_server(port=0)
            # Guardar las credenciales para la próxima ejecución
            with open(self.token_file, 'wb') as token:
                pickle.dump(self.creds, token)
        self.build_service()

    def build_service(self):
        """
        Construye el objeto de servicio usando las credenciales autenticadas.
        """
        try:
            self.service = build(self.service_name, self.version, credentials=self.creds)
            logger.info(f"Servicio {self.service_name} {self.version} creado correctamente.")
        except Exception as e:
            logger.error(f"Error al construir el servicio {self.service_name}: {e}")
            raise e

    def get_service(self):
        """
        Retorna el objeto de servicio autenticado.
        """
        return self.service