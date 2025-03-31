import os

def find_project_root(project_name: str) -> str:
    """
    Busca de forma exhaustiva el directorio con nombre `project_name`.
    - Sube por el árbol de carpetas desde el archivo actual (o cwd).
    - En cada nivel hacia arriba, explora recursivamente hacia abajo.
    - Detiene la búsqueda al encontrar la primera coincidencia exacta.
    
    Returns:
        Ruta absoluta del directorio encontrado.
    
    Raises:
        Exception si no se encuentra ninguna coincidencia.
    """
    try:
        start_dir = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        start_dir = os.getcwd()

    visited = set()
    current_dir = start_dir

    while True:
        # Evitar ciclos
        if current_dir in visited:
            break
        visited.add(current_dir)

        # Recorrer todo desde este nivel hacia abajo
        for root, dirs, _ in os.walk(current_dir):
            if os.path.basename(root) == project_name:
                return root

        # Subir un nivel
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Ya estamos en la raíz
            break
        current_dir = parent_dir

    raise Exception(f"❌ El directorio del proyecto '{project_name}' no se encontró.")
