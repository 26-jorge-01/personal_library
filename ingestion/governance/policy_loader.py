import yaml

def load_policy(policy_path: str) -> dict:
    """
    Carga la política de gobernanza desde un archivo YAML.
    """
    with open(policy_path, "r") as file:
        return yaml.safe_load(file)
