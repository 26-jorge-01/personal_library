# Carpeta de archivos de configuración de los servicios de Google

En esta carpeta se deberán alojar los archivos ```credentials.json``` y ```token.pickle``` generados desde Google Cloud Platform luego de haber parametrizado los scopes necesarios para su uso. 

* **Nota:** En caso de que se genere algún error en uno de los servicios, debes intentar borrar el ```token.pickle``` y volver a aprovar los permisos ya que el token tiene un vencimiento.

En el caso de maps places, se necesitará especificar la dirección IP desde donde se hará la solicitud en el siguiente [enlace](https://console.cloud.google.com/google/maps-apis/credentials?authuser=1&invt=AbtlzQ&project=botzenai).