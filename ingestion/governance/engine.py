import os
import pandas as pd
from ingestion.governance.engine_policy_autogen import (
    get_or_create_policy,
    infer_column_type,
    define_integrity
)
from datetime import datetime
import uuid

PRIVACY_LEVELS = {"high": 3, "medium": 2, "low": 1}
SECURITY_ORDER = {"encrypted": 3, "masked": 2, "none": 1}

class GovernanceEngine:
    def __init__(self, df: pd.DataFrame, policy_filename: str):
        self.df = df
        self.policy = get_or_create_policy(df, policy_filename)
        self.enforcement = self.policy.get("enforcement_requirements", {})
        self.report = []
        self.execution_id = str(uuid.uuid4())
        self.timestamp = datetime.utcnow().isoformat()

    def _add_violation(self, field, issue, severity="warning"):
        self.report.append({
            "field": field,
            "issue": issue,
            "severity": severity,
            "execution_id": self.execution_id,
            "timestamp": self.timestamp
        })

    def validate_field(self, field_policy):
        col_name = field_policy.get("field_name")
        if col_name not in self.df.columns:
            self._add_violation(col_name, "Field is missing from the dataset.", "error")
            return

        series = self.df[col_name]
        inferred_type = infer_column_type(series)
        expected_type = field_policy.get("type")
        if inferred_type != expected_type:
            self._add_violation(col_name, f"Expected type '{expected_type}' but got '{inferred_type}'.")

        if "no_nulls" in field_policy.get("rules", []):
            if series.isnull().any():
                self._add_violation(col_name, "Contains null values.")

        integrity = define_integrity(series)
        if integrity.get("contains_outliers"):
            self._add_violation(col_name, "Contains outliers.")
        if field_policy.get("critical_field") and not integrity.get("unique"):
            self._add_violation(col_name, "Critical field is not unique.")

        for rule in self.enforcement.get("mandatory_rules", []):
            if rule not in field_policy.get("rules", []):
                self._add_violation(col_name, f"Missing mandatory rule '{rule}'.")

        required_privacy = self.enforcement.get("privacy_enforcement", "medium")
        if field_policy.get("data_subject"):
            field_privacy = field_policy.get("privacy_level", "low")
            if PRIVACY_LEVELS.get(field_privacy, 0) < PRIVACY_LEVELS.get(required_privacy, 0):
                self._add_violation(col_name, f"Privacy level '{field_privacy}' is below required '{required_privacy}'.", "error")

        required_security = self.enforcement.get("security_baseline", "masked")
        field_security = field_policy.get("security", "none")
        if SECURITY_ORDER.get(field_security, 0) < SECURITY_ORDER.get(required_security, 0):
            self._add_violation(col_name, f"Security level '{field_security}' is below baseline '{required_security}'.")

        allowed_transparency = self.enforcement.get("allowed_transparency", ["internal", "public"])
        if field_policy.get("transparency", "internal") not in allowed_transparency:
            self._add_violation(col_name, f"Transparency '{field_policy.get('transparency')}' not allowed.")

        required_frameworks = set(self.enforcement.get("framework_enforcement", []))
        field_compliance = set(field_policy.get("compliance_tags", []))
        if field_policy.get("data_subject") and not required_frameworks.issubset(field_compliance):
            self._add_violation(col_name, f"Missing required compliance frameworks: {required_frameworks - field_compliance}.")

    def validate_global(self):
        dataset_risk = self.policy.get("compliance", {}).get("risk_level", "low")
        accepted_risk = self.enforcement.get("risk_acceptance", "medium")
        risk_order = {"low": 1, "medium": 2, "high": 3}
        if risk_order.get(dataset_risk, 0) > risk_order.get(accepted_risk, 0):
            self._add_violation("__global__", f"Dataset risk level '{dataset_risk}' exceeds accepted '{accepted_risk}'.", "error")

    def run_policy_checks(self):
        for field in self.policy.get("fields", []):
            self.validate_field(field)
        self.validate_global()
        return self.report
