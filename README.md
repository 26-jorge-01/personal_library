# personal-library

Este repositorio es la base para la construcción de proyectos de datos reutilizables y escalables. Está diseñado para facilitar la ingesta, carga, transformación y manejo de datos, con un enfoque especial en la calidad, gobernabilidad y trazabilidad desde el primer paso del proceso.

---

## Visión General

El **Data Project Hub** es una plataforma modular que agrupa componentes reutilizables para:

- **Ingesta:** Extraer datos desde diversas fuentes (por ejemplo, Socrata, archivos CSV, APIs REST, bases de datos, etc.) con mecanismos robustos de error, reintentos y logging.
- **Loaders:** Cargar datos desde múltiples orígenes, implementando conectores personalizados y genéricos.
- **Transformers:** Aplicar transformaciones, limpieza y enriquecimiento a los datos para prepararlos para análisis o cargas en sistemas de destino.
- **Utils:** Funciones generales y herramientas auxiliares que facilitan tareas comunes en todo el pipeline.
- **Demos:** Ejemplos y notebooks interactivos que ilustran el uso e integración de cada componente.

Esta estructura permite reutilizar, escalar y mantener de manera efectiva todo el ciclo de vida del procesamiento de datos.

---

## Estructura del Proyecto

project_root/
├── demos/ # Notebooks y scripts demostrativos para validar y ejemplificar el uso de cada componente. 
├── ingestion/ # Módulos para la ingesta de datos. Incluye conectores (por ejemplo, desde Socrata) y motores de gobernanza. 
├── loaders/ # Conectores y módulos para cargar datos desde diversas fuentes (archivos, bases de datos, etc.). 
├── transformers/ # Componentes para transformar y procesar datos: limpieza, normalización, enriquecimiento, etc. 
├── utils/ # Utilidades y funciones auxiliares compartidas por todo el proyecto (logging, configuración, helpers). 
└── README.md # Este documento, que explica la visión, estructura y uso del proyecto.

Cada carpeta está pensada para ser escalable. Por ejemplo, mientras que `ingestion` se encarga de la extracción y validación de datos, las carpetas `loaders`, `transformers` y `utils` se ampliarán conforme se desarrollen nuevas funcionalidades.

---

## Características Principales

- **Modularidad y Reutilización:**  
  Cada componente está desacoplado, permitiendo agregar nuevos conectores, transformaciones o utilidades sin modificar la base del proyecto.

- **Gobernabilidad y Calidad:**  
  La ingesta incorpora mecanismos para validar la calidad de los datos (reglas de nulidad, unicidad, rangos, formatos, outliers, integridad referencial, etc.) y registrar metadatos de cada proceso para una auditoría completa.

- **Escalabilidad:**  
  La arquitectura permite extender el sistema a nuevos orígenes y procesos ETL, integrándose fácilmente con pipelines complejos (como sistemas RAG o flujos en n8n).

- **Interactividad:**  
  Los notebooks en la carpeta `demos` ofrecen ejemplos prácticos y documentación interactiva para comprender el flujo de trabajo y acelerar la adopción del framework.

---

## Requisitos Previos

- **Python:** Versión 3.7 o superior.
- **Dependencias Principales:**
  - `pandas`
  - `sodapy`
  - `pyyaml`
  - `requests`
  - `numpy`

Para instalar las dependencias, ejecuta:

```bash
pip install -r requirements.txt
```

## Instalación y Configuración

Clona el repositorio:

```bash
git clone <URL_DEL_REPOSITORIO>
cd <NOMBRE_DEL_REPOSITORIO>
```

Configura el entorno de desarrollo (opcional):

```bash
python -m venv venv
source venv/bin/activate   # Linux/Mac
venv\Scripts\activate      # Windows
```

Instala las dependencias:

```bash
pip install -r requirements.txt
```

Configura las Políticas de Gobernanza:

Edita o crea archivos de política YAML en ingestion/governance/policies/ para definir las reglas de validación y gobernabilidad que se aplicarán a cada fuente de datos.

## Guía de Uso

### Ejecución del Pipeline de Ingesta

El proceso completo se orquesta a través del script principal ubicado en ingestion/main_ingestion.py. Al ejecutarlo, el sistema:

- Se conecta a la fuente de datos (por ejemplo, Socrata) utilizando conectores en ingestion o loaders.
- Aplica validaciones y políticas de calidad mediante el motor de gobernanza.
- Registra metadatos y genera un archivo de auditoría (por ejemplo, en reports/audit_log.parquet).

Para ejecutar el pipeline, utiliza:

```bash
python ingestion/main_ingestion.py
```

### Uso Interactivo

La carpeta demos contiene notebooks (como demo_socrata.ipynb) que ilustran:

- Cómo configurar y ejecutar la extracción de datos.

- Cómo aplicar transformaciones y validaciones.

- Cómo integrar otros componentes del proyecto.


Estos demos están diseñados para facilitar la comprensión del flujo y servir de base para futuras extensiones.

## Extensión y Personalización

### Nuevos Conectores (Loaders):

Agrega módulos en la carpeta loaders/ para conectar a nuevas fuentes de datos (bases de datos, APIs, archivos, etc.).

### Nuevos Transformadores (Transformers):

Desarrolla pipelines de transformación en transformers/ para limpiar, normalizar y enriquecer los datos según los requerimientos.

### Utilidades Comunes (Utils):

Centraliza funciones y herramientas de uso común en la carpeta utils/.

### Gobernanza:

Las políticas y reglas definidas en ingestion/governance/ se configuran mediante archivos YAML, permitiendo adaptar rápidamente las validaciones sin modificar el código.

## Buenas Prácticas

### Modulariza y Desacopla:

Mantén la lógica de ingesta, transformación, carga y utilidades separada para facilitar el mantenimiento y la escalabilidad.

### Audita y Valida:

Revisa regularmente los logs de auditoría y reportes de calidad para detectar y corregir anomalías en los datos.

### Documenta los Cambios:

Actualiza este README y la documentación interna conforme se agreguen nuevas funcionalidades o se modifiquen procesos.

### Control de Versiones:

Utiliza Git para el control de versiones y documenta cada cambio significativo en el código y las políticas.

## Contribución

Las contribuciones son bienvenidas. Para colaborar en este proyecto:

- Sigue los estándares de codificación (PEP8).
- Agrega tests y documentación para nuevas funcionalidades.
- Abre Pull Requests con descripciones claras y detalladas de tus cambios.

Mantén actualizada la documentación del proyecto.

## Licencia

Este proyecto se distribuye bajo la Licencia MIT. Consulta el archivo LICENSE para más detalles.

## Contacto

Para preguntas, sugerencias o reportar problemas, por favor contacta a:

Nombre: [Tu Nombre]
Email: [tu.email@example.com]