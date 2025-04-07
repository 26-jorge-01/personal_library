import os
import re
import yaml
import pandas as pd
from datetime import datetime
from governance.rules import AVAILABLE_RULES
from utils.file_management.folder_searcher import find_or_create_folder

POLICY_FOLDER = os.path.join(os.path.dirname(__file__), "policies")


SENSITIVE_PATTERNS = {
    "email": re.compile(r"^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$"),
    "phone": re.compile(r"\+?\d{7,15}"),
    "ssn": re.compile(r"\d{3}-\d{2}-\d{4}"),
    "id": re.compile(r"\d{6,10}")
}

# GDPR = General Data Protection Regulation
# Se consideran identificadores personales como sensibles y sujetos a regulaciones de privacidad.
# En este caso, se consideran términos relacionados con la identificación personal y la localización.
# Se pueden incluir otros identificadores como el número de pasaporte, licencia de conducir, etc.
GDPR_IDENTIFIERS = ["name", "email", "address", "location", "phone", "id"]

# HIPAA = Health Insurance Portability and Accountability Act
# Se consideran identificadores personales como sensibles y sujetos a regulaciones de privacidad.
# En este caso, se consideran términos relacionados con la salud y el tratamiento médico.
# Se pueden incluir otros identificadores como el número de historia clínica, diagnóstico, tratamiento, etc.
HIPAA_IDENTIFIERS = ["ssn", "diagnosis", "treatment", "health", "insurance"]

def infer_column_type(series):
    if pd.api.types.is_integer_dtype(series):
        return "integer"
    elif pd.api.types.is_float_dtype(series):
        return "float"
    elif pd.api.types.is_bool_dtype(series):
        return "boolean"
    elif pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def infer_rules_by_type(col_type, series):
    rules = []
    if col_type == "string":
        rules.extend(["no_nulls", "no_special_characters", "max_length"])
    elif col_type in ["integer", "float"]:
        rules.append("no_nulls")
        if (series >= 0).all():
            rules.append("positive_values")
        if series.nunique() < 10:
            rules.append("enumerated_values")
    elif col_type == "datetime":
        rules.extend(["valid_date_format", "no_nulls"])
    elif col_type == "boolean":
        rules.append("no_nulls")
    return [r for r in rules if r in AVAILABLE_RULES]


def detect_sensitive_content(series):
    sample = series.dropna().astype(str).head(50)
    for pattern in SENSITIVE_PATTERNS.values():
        if sample.apply(lambda x: bool(pattern.match(x))).any():
            return True
    return False


def is_embedded_sensitive(series):
    sample = series.dropna().astype(str).head(50)
    combined = " ".join(sample.values)
    return any(pattern.search(combined) for pattern in SENSITIVE_PATTERNS.values())


def detect_outliers(series):
    # Si la serie no es numérica o booleana, no se consideran outliers
    if not pd.api.types.is_numeric_dtype(series) or pd.api.types.is_bool_dtype(series):
        return False
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    outliers = ((series < q1 - 1.5 * iqr) | (series > q3 + 1.5 * iqr)).sum()
    return outliers > 0


def define_privacy_level(field, series):
    lower = field.lower()
    if any(word in lower for word in ["name", "email", "phone", "address", "id", "ssn", "passport"]):
        return "high"
    if detect_sensitive_content(series) or is_embedded_sensitive(series):
        return "high"
    if any(word in lower for word in ["age", "gender", "location"]):
        return "medium"
    return "low"


def define_security_requirement(privacy_level):
    return {
        "high": "encrypted",
        "medium": "masked",
        "low": "none"
    }.get(privacy_level, "none")


def define_transparency(field):
    lower = field.lower()
    if "description" in lower or "category" in lower:
        return "public"
    return "internal"


def define_integrity(series):
    try:
        unique = bool(series.is_unique)
    except TypeError:
        unique = False
    return {
        "unique": unique,
        "no_nulls": bool(not series.isnull().any()),
        "consistent_format": True,
        "contains_outliers": bool(detect_outliers(series))
    }


def define_compliance_tags(field):
    tags = []
    lower = field.lower()
    if any(word in lower for word in GDPR_IDENTIFIERS):
        tags.append("GDPR")
    if any(word in lower for word in HIPAA_IDENTIFIERS):
        tags.append("HIPAA")
    return tags


def define_access_restriction(privacy_level):
    return {
        "high": "restricted",
        "medium": "internal",
        "low": "public"
    }.get(privacy_level, "internal")

def validate_compliance(policy: dict, region: str = "CO") -> list:
    """
    Valida que las políticas definidas en YAML cumplan con normativas como Habeas Data, GDPR, etc.
    Devuelve una lista de advertencias o errores.
    """
    issues = []
    fields = policy.get("fields", {})
    for field, config in fields.items():
        if config.get("sensitive") and config.get("policy") not in ["mask", "drop", "anonymize"]:
            issues.append(f"Campo sensible '{field}' sin tratamiento apropiado en política de privacidad.")
        if config.get("required") and not config.get("policy"):
            issues.append(f"Campo obligatorio '{field}' sin política asignada.")
        if region == "CO" and config.get("compliance") != "habeas_data":
            issues.append(f"Campo '{field}' sin marcado de cumplimiento de Habeas Data para región CO.")
    return issues


def generate_default_policy(df: pd.DataFrame, policy_filename: str):
    # Tipado forzado antes de procesar la muestra
    def force_cast_column(col):
        try:
            return pd.to_numeric(col, errors='raise')
        except:
            try:
                return pd.to_datetime(col, errors='raise')
            except:
                try:
                    if col.dropna().isin(["True", "False", True, False]).all():
                        return col.map(lambda x: True if str(x).lower() == "true" else False)
                except:
                    pass
        return col

    df = df.copy()
    for col in df.columns:
        df[col] = force_cast_column(df[col])
    sample = df.sample(min(len(df), 500))

    policy = {
        "dataset_metadata": {
            "columns": len(df.columns),
            "rows_sampled": len(sample),
            "generated_by": "default_policy_generator",
            "generated_on": datetime.utcnow().isoformat() + "Z",
            "data_source": "inferred",
            "schema_version": "1.0",
            "language": "undetected"
        },
        "compliance": {
            "compliance_frameworks": [],
            "risk_level": "low"
        },
        "enforcement_requirements": {
            "mandatory_rules": ["no_nulls", "valid_date_format"],
            "required_fields": [],
            "privacy_enforcement": "medium",
            "security_baseline": "masked",
            "allowed_transparency": ["internal", "public"],
            "risk_acceptance": "medium",
            "framework_enforcement": ["GDPR"]
        },
        "fields": []
    }

    privacy_counts = {"high": 0, "medium": 0, "low": 0}
    frameworks_detected = set()

    for col in sample.columns:
        col_type = infer_column_type(sample[col])
        rules = infer_rules_by_type(col_type, sample[col])
        privacy = define_privacy_level(col, sample[col])
        security = define_security_requirement(privacy)
        transparency = define_transparency(col)
        integrity = define_integrity(sample[col])
        compliance_tags = define_compliance_tags(col)
        access_restriction = define_access_restriction(privacy)

        for tag in compliance_tags:
            frameworks_detected.add(tag)

        privacy_counts[privacy] += 1

        field_policy = {
            "field_name": col,
            "type": col_type,
            "required": True,
            "rules": rules,
            "privacy_level": privacy,
            "security": security,
            "transparency": transparency,
            "integrity": integrity,
            "compliance_tags": compliance_tags,
            "data_subject": bool(compliance_tags),
            "access_restriction": access_restriction,
            "retention_policy": "default",
            "critical_field": col.lower() in ["id", "user_id", "ssn"],
            "justification": f"Field '{col}' classified as {col_type} with privacy={privacy} based on naming and content analysis."
        }

        policy["fields"].append(field_policy)

    if privacy_counts["high"] >= 2:
        policy["compliance"]["risk_level"] = "high"
    elif privacy_counts["medium"] >= 2:
        policy["compliance"]["risk_level"] = "medium"

    policy["compliance"]["compliance_frameworks"] = sorted(list(frameworks_detected))

    os.makedirs(POLICY_FOLDER, exist_ok=True)
    policy_path = os.path.join(POLICY_FOLDER, policy_filename)
    with open(policy_path, "w") as f:
        yaml.dump(policy, f, sort_keys=False, allow_unicode=True)

    return policy

def load_policy(policy_path: str) -> dict:
    with open(policy_path, "r") as file:
        return yaml.safe_load(file)

def get_or_create_policy(df: pd.DataFrame, policy_filename: str):
    policy_path = find_or_create_folder('policies')
    policy_path = os.path.join(policy_path, policy_filename)

    if os.path.exists(policy_path):
        return load_policy(policy_path)
    else:
        print(f"[INFO] Policy '{policy_filename}' not found. Generating default...")
        return generate_default_policy(df, policy_filename)
