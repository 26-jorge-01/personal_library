import os

def find_project_root(project_name: str) -> str:
    """
    Navega hacia arriba desde el directorio actual hasta encontrar el directorio 
    cuyo nombre coincide con `project_name`. Retorna la ruta absoluta de ese directorio.
    
    Raises:
        Exception: Si no se encuentra el directorio con el nombre especificado.
    """
    try:
        # Usar __file__ si está definido, en caso contrario usar el directorio de trabajo actual.
        current_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_dir = os.getcwd()
    
    while True:
        if os.path.basename(current_dir) == project_name:
            return current_dir
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Se alcanzó la raíz del sistema de archivos
            raise Exception(f"El directorio del proyecto '{project_name}' no se encontró.")
        current_dir = parent_dir