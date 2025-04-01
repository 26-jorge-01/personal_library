from utils.google.base import GoogleServiceBase
import logging

# Configuración básica del logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class GmailService(GoogleServiceBase):
    """
    Cliente completo para la API de Gmail que permite interactuar con todos sus endpoints.
    
    Esta clase expone métodos para:
      - Obtener el perfil de usuario.
      - Listar, crear, actualizar y borrar etiquetas.
      - Listar, obtener, enviar, modificar, borrar, archivar (trash/untrash) mensajes.
      - Gestionar threads (listar, obtener, modificar y borrar).
      - Gestionar borradores (listar, obtener, crear, enviar y borrar).
    
    Los scopes incluyen permisos para lectura, modificación y envío.
    """
    def __init__(self, credentials_file='credentials.json', token_file='gmail_token.pickle'):
        scopes = [
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/gmail.send',
            'https://www.googleapis.com/auth/gmail.labels',
            'https://www.googleapis.com/auth/gmail.readonly'
        ]
        super().__init__(service_name='gmail', version='v1', scopes=scopes,
                         credentials_file=credentials_file, token_file=token_file)

    # Perfil
    def get_profile(self):
        try:
            return self.service.users().getProfile(userId='me').execute()
        except Exception as e:
            logger.error("Error al obtener el perfil de Gmail: %s", e)
            return None

    # Etiquetas
    def list_labels(self):
        try:
            results = self.service.users().labels().list(userId='me').execute()
            return results.get('labels', [])
        except Exception as e:
            logger.error("Error al listar etiquetas: %s", e)
            return None

    def get_label(self, label_id):
        try:
            return self.service.users().labels().get(userId='me', id=label_id).execute()
        except Exception as e:
            logger.error("Error al obtener la etiqueta %s: %s", label_id, e)
            return None

    def create_label(self, label_data):
        try:
            return self.service.users().labels().create(userId='me', body=label_data).execute()
        except Exception as e:
            logger.error("Error al crear etiqueta: %s", e)
            return None

    def update_label(self, label_id, label_data):
        try:
            return self.service.users().labels().update(userId='me', id=label_id, body=label_data).execute()
        except Exception as e:
            logger.error("Error al actualizar la etiqueta %s: %s", label_id, e)
            return None

    def delete_label(self, label_id):
        try:
            return self.service.users().labels().delete(userId='me', id=label_id).execute()
        except Exception as e:
            logger.error("Error al borrar la etiqueta %s: %s", label_id, e)
            return None

    # Mensajes
    def list_messages(self, query=None, max_results=100):
        try:
            response = self.service.users().messages().list(userId='me', q=query, maxResults=max_results).execute()
            return response.get('messages', [])
        except Exception as e:
            logger.error("Error al listar mensajes: %s", e)
            return None

    def get_message(self, message_id, format='full'):
        try:
            return self.service.users().messages().get(userId='me', id=message_id, format=format).execute()
        except Exception as e:
            logger.error("Error al obtener el mensaje %s: %s", message_id, e)
            return None

    def send_message(self, raw_message):
        """
        Envía un mensaje.
        
        Parámetro:
          - raw_message: Cadena codificada en base64 URL-safe que representa el mensaje MIME.
        """
        try:
            body = {'raw': raw_message}
            return self.service.users().messages().send(userId='me', body=body).execute()
        except Exception as e:
            logger.error("Error al enviar mensaje: %s", e)
            return None

    def delete_message(self, message_id):
        try:
            return self.service.users().messages().delete(userId='me', id=message_id).execute()
        except Exception as e:
            logger.error("Error al borrar mensaje %s: %s", message_id, e)
            return None

    def modify_message(self, message_id, add_labels=None, remove_labels=None):
        try:
            body = {}
            if add_labels:
                body['addLabelIds'] = add_labels
            if remove_labels:
                body['removeLabelIds'] = remove_labels
            return self.service.users().messages().modify(userId='me', id=message_id, body=body).execute()
        except Exception as e:
            logger.error("Error al modificar mensaje %s: %s", message_id, e)
            return None

    def trash_message(self, message_id):
        try:
            return self.service.users().messages().trash(userId='me', id=message_id).execute()
        except Exception as e:
            logger.error("Error al archivar mensaje %s: %s", message_id, e)
            return None

    def untrash_message(self, message_id):
        try:
            return self.service.users().messages().untrash(userId='me', id=message_id).execute()
        except Exception as e:
            logger.error("Error al restaurar mensaje %s: %s", message_id, e)
            return None

    # Threads
    def list_threads(self, query=None, max_results=100):
        try:
            response = self.service.users().threads().list(userId='me', q=query, maxResults=max_results).execute()
            return response.get('threads', [])
        except Exception as e:
            logger.error("Error al listar threads: %s", e)
            return None

    def get_thread(self, thread_id, format='full'):
        try:
            return self.service.users().threads().get(userId='me', id=thread_id, format=format).execute()
        except Exception as e:
            logger.error("Error al obtener thread %s: %s", thread_id, e)
            return None

    def modify_thread(self, thread_id, add_labels=None, remove_labels=None):
        try:
            body = {}
            if add_labels:
                body['addLabelIds'] = add_labels
            if remove_labels:
                body['removeLabelIds'] = remove_labels
            return self.service.users().threads().modify(userId='me', id=thread_id, body=body).execute()
        except Exception as e:
            logger.error("Error al modificar thread %s: %s", thread_id, e)
            return None

    def delete_thread(self, thread_id):
        try:
            return self.service.users().threads().delete(userId='me', id=thread_id).execute()
        except Exception as e:
            logger.error("Error al borrar thread %s: %s", thread_id, e)
            return None

    # Borradores (Drafts)
    def list_drafts(self, max_results=100):
        try:
            response = self.service.users().drafts().list(userId='me', maxResults=max_results).execute()
            return response.get('drafts', [])
        except Exception as e:
            logger.error("Error al listar borradores: %s", e)
            return None

    def get_draft(self, draft_id):
        try:
            return self.service.users().drafts().get(userId='me', id=draft_id).execute()
        except Exception as e:
            logger.error("Error al obtener borrador %s: %s", draft_id, e)
            return None

    def create_draft(self, message):
        """
        Crea un borrador.
        
        Parámetro:
          - message: Diccionario que representa el mensaje (normalmente debe incluir la clave 'raw').
        """
        try:
            body = {'message': message}
            return self.service.users().drafts().create(userId='me', body=body).execute()
        except Exception as e:
            logger.error("Error al crear borrador: %s", e)
            return None

    def send_draft(self, draft_id):
        try:
            return self.service.users().drafts().send(userId='me', id=draft_id).execute()
        except Exception as e:
            logger.error("Error al enviar borrador %s: %s", draft_id, e)
            return None

    def delete_draft(self, draft_id):
        try:
            return self.service.users().drafts().delete(userId='me', id=draft_id).execute()
        except Exception as e:
            logger.error("Error al borrar borrador %s: %s", draft_id, e)
            return None
