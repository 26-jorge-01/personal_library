import pandas as pd
import logging
import re
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import gc

logger = logging.getLogger(__name__)

def clean_special_characters(series: pd.Series) -> pd.Series:
    """
    Limpia profundamente una serie de texto eliminando caracteres especiales, acentos,
    emojis, stopwords y aplicando reglas específicas de reemplazo.

    Parámetros:
      - series: Serie de pandas con los datos.

    Retorna:
      - Serie de pandas con el texto limpio.
    """
    try:
        stop_words = set(stopwords.words("spanish"))
        series = series.dropna().astype(str)

        # Reglas previas de reemplazo
        series = series.str.lower().replace({
            'definido': 'nodefinido',
            'sin descripcion': 'sindescripcion',
            'no definido': 'nodefinido',
            'no aplica': 'noaplica'
        })
        
        def process_text(text):
            text = re.sub(r'(?::|;|=)(?:-)?(?:\)|\(|D|P)', "", text)
            text = re.sub(r'[\\!\\"\\#\\$\\%\\&\\\'\\(\\)\\*\\+\\,\\-\\.\\/\\:\\;\\<\\=\\>\\?\\@\\[\\\\\\]\\^_\\`\\{\\|\\}\\~]', "", text)
            text = re.sub(r'\#\.', '', text)
            text = re.sub(r'\n', ' ', text)
            text = re.sub(r'  ', ' ', text)
            text = re.sub(r'´', '',text)
            text = re.sub(r',', '',text)
            text = re.sub(r'\-', '', text)
            text = re.sub(r'�', '', text)
            text = re.sub(r'á', 'a', text)
            text = re.sub(r'é', 'e', text)
            text = re.sub(r'í', 'i', text)
            text = re.sub(r'ó', 'o', text)
            text = re.sub(r'ú', 'u', text)
            text = re.sub(r'ò', 'o', text)
            text = re.sub(r'à', 'a', text)
            text = re.sub(r'è', 'e', text)
            text = re.sub(r'ì', 'i', text)
            text = re.sub(r'ù', 'u', text)
            text = re.sub("\\s+", ' ', text)

            tokens = word_tokenize(text)
            tokens = [w for w in tokens if w not in stop_words]
            return " ".join(tokens)

        # Obtener valores únicos limpios
        unique_values = series.unique()
        cleaned_map = {val: process_text(val) for val in unique_values}
        series = series.map(cleaned_map)

        # Reglas inversas de restauración
        series = series.replace({
            'nodefinido': 'no definido',
            'noaplica': 'no aplica',
            'sindescripcion': 'sin descripcion'
        })

        gc.collect()
        return series, "Limpieza de caracteres especiales"

    except Exception as e:
        logger.error("Error en clean_text_series: %s", str(e))
        return series, "Error en limpieza de caracteres especiales"