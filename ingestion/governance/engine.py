from ingestion.governance import rules

class GovernanceEngine:
    """
    Motor de gobernanza que aplica validaciones sobre el DataFrame
    utilizando políticas configuradas, tanto para reglas de campo individual
    como para reglas que involucran múltiples campos.
    """
    def __init__(self, df, policy: dict):
        self.df = df
        self.policy = policy
        self.report = {"errors": [], "warnings": []}

    def validate(self):
        self._check_required_fields()
        self._check_types()
        self._apply_rules()
        self._apply_inter_field_rules()
        return self.df, self.report

    def _check_required_fields(self):
        for field in self.policy.get("required_fields", []):
            if field not in self.df.columns:
                self.report["errors"].append(f"Falta el campo obligatorio: {field}")

    def _check_types(self):
        expected_types = self.policy.get("expected_types", {})
        for field, expected in expected_types.items():
            if field in self.df.columns:
                actual = str(self.df[field].dtype)
                if expected not in actual:
                    self.report["warnings"].append(
                        f"El campo '{field}' tiene tipo '{actual}', se esperaba '{expected}'."
                    )

    def _apply_rules(self):
        # Reglas definidas para cada campo
        for field, rule_def in self.policy.get("rules", {}).items():
            if isinstance(rule_def, str):
                # Regla simple: solo se indica el nombre
                rule_name = rule_def
                func = getattr(rules, f"rule_{rule_name}", None)
                if callable(func):
                    result = func(self.df, field)
                    if result:
                        self.report["warnings"].append(result)
            elif isinstance(rule_def, dict):
                # Regla con parámetros: se extrae el nombre y se pasan parámetros extra
                rule_name = rule_def.get("rule")
                if not rule_name:
                    continue
                func = getattr(rules, f"rule_{rule_name}", None)
                if callable(func):
                    params = {k: v for k, v in rule_def.items() if k != "rule"}
                    result = func(self.df, field, **params)
                    if result:
                        self.report["warnings"].append(result)

    def _apply_inter_field_rules(self):
        # Reglas que involucran más de un campo
        for rule_def in self.policy.get("inter_field_rules", []):
            rule_name = rule_def.get("rule")
            fields = rule_def.get("fields")
            func = getattr(rules, f"rule_{rule_name}", None)
            if callable(func):
                params = {k: v for k, v in rule_def.items() if k not in ["rule", "fields"]}
                result = func(self.df, fields, **params)
                if result:
                    self.report["warnings"].append(result)
