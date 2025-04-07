import os

def find_project_root(project_name: str = None) -> str:
    """
    Busca de forma exhaustiva el directorio de un proyecto.
    
    Si se proporciona 'project_name', se busca el directorio cuyo nombre coincida exactamente.
    Si no se proporciona, se recorre el árbol de directorios subiendo desde el directorio actual 
    y se retorna la primera ruta que contenga una carpeta o archivo '.git'.
    
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
            if project_name:
                if os.path.basename(root) == project_name:
                    return root
            else:
                # Buscamos la carpeta o archivo .git
                if ".git" in dirs or os.path.exists(os.path.join(root, ".git")):
                    return root

        # Subir un nivel
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:  # Ya estamos en la raíz del sistema
            break
        current_dir = parent_dir

    if project_name:
        raise Exception(f"❌ El directorio del proyecto '{project_name}' no se encontró.")
    else:
        raise Exception("❌ No se encontró ningún directorio que contenga '.git'.")
    
def find_or_create_folder(folder_name: str, project_name: str = None) -> str:
    """
    Busca de forma recursiva en el proyecto, a partir de una raíz 'root',
    una carpeta que coincida exactamente con 'folder_name'. 
    Si la encuentra, retorna su ruta.
    Si no la encuentra, la crea en la raíz del proyecto y retorna la nueva ruta.
    """
    root = find_project_root(project_name)
    for dirpath, dirnames, _ in os.walk(root):
        if folder_name in dirnames:
            return os.path.join(dirpath, folder_name)
    
    # Si no se encontró, se crea la carpeta en la raíz del proyecto.
    new_folder = os.path.join(root, folder_name)
    os.makedirs(new_folder, exist_ok=True)
    return new_folder
