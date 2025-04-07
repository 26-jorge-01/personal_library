# Módulos base
import pandas as pd
import logging
import os
import json
# Módulos personales
from governance.quality_management.data_quality_checks.advance_quality_report import QualityReportEngine
from governance.quality_management.data_remediation.base_remediation_engine import AdvancedDataRemediationEngine
# Normalización
from governance.quality_management.data_remediation.normalization.performance import evaluate_normalization, select_best_normalization
from governance.quality_management.data_remediation.normalization.rules.numeric import normalize_minmax, normalize_zscore
from governance.quality_management.data_remediation.normalization.rules.string import clean_special_characters
from governance.quality_management.data_remediation.normalization.rules.datetime import normalize_datetime
# Imputación
from governance.quality_management.data_remediation.imputation.performance import evaluate_imputation, select_best_imputation
from governance.quality_management.data_remediation.imputation.rules.numeric import impute_with_median
from governance.quality_management.data_remediation.imputation.rules.datetime import impute_mode_date, impute_default_date
from governance.quality_management.data_remediation.imputation.rules.boolean import impute_boolean_false, impute_boolean_true
from governance.quality_management.data_remediation.imputation.rules.string import impute_empty_string
# Atípicos
from governance.quality_management.data_remediation.atypical.performance import select_best_outlier_handling, evaluate_outlier_handling
from governance.quality_management.data_remediation.atypical.rules.numeric import winsorize_iqr
# Sesgo
from governance.quality_management.data_remediation.bias.performance import select_best_bias, evaluate_bias
from governance.quality_management.data_remediation.bias.rules.numeric import reduce_skewness_log, reduce_skewness_boxcox, reduce_skewness_yeojohnson, quantile_normalization
from governance.quality_management.data_remediation.bias.rules.string import *
from governance.quality_management.data_remediation.bias.rules.datetime import reduce_temporal_skewness, cyclical_encoding

# Configuración básica del logger
logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "max_epochs": 5,
    "improvement_threshold": 0.5,
    "knowledge_file": "remediation_knowledge.json",
    "include_fields": None,
    "exclude_fields": None
}

class IterativeRemediationEngine:
    def __init__(self, df: pd.DataFrame, config: dict = None):
        conf = DEFAULT_CONFIG.copy()
        if config:
            conf.update(config)
        self.original_df = df.copy()
        self.max_epochs = conf.get("max_epochs", 5)
        self.improvement_threshold = conf.get("improvement_threshold", 0.5)
        self.knowledge_file = conf.get("knowledge_file", "remediation_knowledge.json")
        self.include_fields = conf.get("include_fields", None)
        self.exclude_fields = conf.get("exclude_fields", None)
        self.iteration_logs = []      # Lista de tuplas (epoch, quality_score_global)
        self.iteration_history = []   # Historial completo de quality_report por iteración
        self.knowledge_base = self.load_knowledge()

        # Reglas variantes
        self.technique_variants = {
            "numeric": [
                # Imputación
                {"name": "impute_median", "func": impute_with_median},
                # Atípicos
                {"name": "atypical_winsorization_iqr", "func": winsorize_iqr},
                # Normalización
                {"name": "normalize_minmax", "func": normalize_minmax},
                {"name": "normalize_zscore", "func": normalize_zscore},
                # Sesgo
                {"name": "bias_reduce_skewness_log", "func": reduce_skewness_log},
                {"name": "bias_reduce_skewness_boxcox", "func": reduce_skewness_boxcox},
                {"name": "bias_reduce_skewness_yeojohnson", "func": reduce_skewness_yeojohnson},
                {"name": "bias_quantile_normalization", "func": quantile_normalization}
            ],
            "datetime": [
                # Imputación
                {"name": "impute_default_date", "func": impute_default_date},
                {"name": "impute_mode_date", "func": impute_mode_date},
                # Sesgo
                {"name": "bias_reduce_temporal_skewness", "func": reduce_temporal_skewness},
                {"name": "bias_cyclical_encoding", "func": cyclical_encoding}
            ],
            "boolean": [
                # Imputación
                {"name": "impute_boolean_false", "func": impute_boolean_false},
                {"name": "impute_boolean_true", "func": impute_boolean_true}
            ],
            "string": [
                # Sesgo
                {"name": "bias_noop", "func": noop_string_bias},
                {"name": "bias_group_rare_categories", "func": group_rare_categories},
                {"name": "bias_merge_similar_categories", "func": merge_similar_categories}
            ]
        }
        # Reglas obligatorias
        self.mandatory_rules = {
            "numeric": [],
            "datetime": [
                # # Imputación
                # {"name": "impute_default_date", "func": impute_default_date},
                # # Normalización
                # {"name": "normalize_datetime", "func": normalize_datetime}
            ],
            "boolean": [],
            "string": [
                # Imputación
                {"name": "impute_empty_string", "func": impute_empty_string},
                # Normalización
                {"name": "clean_special_characters", "func": clean_special_characters}
            ]
        }

    def get_type_group(self, inferred_type: str):
        if inferred_type in ["integer", "float"]:
            return "numeric"
        elif inferred_type == "datetime":
            return "datetime"
        elif inferred_type == "boolean":
            return "boolean"
        elif inferred_type == "string":
            return "string"
        else:
            return None

    def register_remediation_rule(self, type_group: str, rule_name: str, func):
        logger.info("Registrando nueva regla de remediación para %s: %s", type_group, rule_name)
        variant = {"name": rule_name, "func": func}
        if type_group in self.technique_variants:
            self.technique_variants[type_group].append(variant)
        else:
            self.technique_variants[type_group] = [variant]

    def register_mandatory_rule(self, type_group: str, rule_name: str, func):
        logger.info("Registrando regla obligatoria para %s: %s", type_group, rule_name)
        rule = {"name": rule_name, "func": func}
        if type_group in self.mandatory_rules:
            self.mandatory_rules[type_group].append(rule)
        else:
            self.mandatory_rules[type_group] = [rule]

    def load_knowledge(self):
        if os.path.exists(self.knowledge_file):
            try:
                with open(self.knowledge_file, 'r') as f:
                    kb = json.load(f)
                logger.info("Base de conocimiento cargada desde %s", self.knowledge_file)
                return kb
            except Exception as e:
                logger.error("Error al cargar conocimiento: %s", str(e))
        return {}

    def save_knowledge(self):
        try:
            with open(self.knowledge_file, 'w') as f:
                json.dump(self.knowledge_base, f, indent=4)
            logger.info("Base de conocimiento guardada en %s", self.knowledge_file)
        except Exception as e:
            logger.error("Error al guardar conocimiento: %s", str(e))

    def apply_mandatory_rules(self, current_df, report):
        for col in current_df.columns:
            if self.include_fields is not None and col not in self.include_fields:
                logger.info("Columna %s excluida por la lista de inclusión.", col)
                continue
            if self.exclude_fields is not None and col in self.exclude_fields:
                logger.info("Columna %s excluida por la lista de exclusión.", col)
                continue

            col_report = report.get(col, {})
            inferred_type = col_report.get("inferred_type", "unknown")
            type_group = self.get_type_group(inferred_type)
            if type_group and self.mandatory_rules.get(type_group):
                for rule in self.mandatory_rules[type_group]:
                    candidate_series = current_df[col].copy()
                    candidate_series, action_desc = rule["func"](candidate_series)
                    current_df[col] = candidate_series
                    logger.info("Columna %s: regla obligatoria %s aplicada: %s", col, rule["name"], action_desc)
                    self.knowledge_base.setdefault(col, {}).setdefault("mandatory", []).append(rule["name"])
        return current_df

    def run(self):
        # Copiar el DataFrame original y generar el quality_report inicial.
        current_df = self.original_df.copy()
        prev_report = QualityReportEngine(current_df).generate_report()
        prev_global_score = prev_report["global"]["average_quality_score"]
        logger.info("Quality Score inicial: %.2f", prev_global_score)
        self.iteration_logs.append(("Inicial", prev_global_score))
        self.iteration_history.append({"epoch": "Inicial", "report": prev_report})
    
        # Aplicar las reglas obligatorias previamente registradas.
        current_df = self.apply_mandatory_rules(current_df, prev_report)
    
        # Definir funciones helper internas para evaluar y aplicar candidatos.
        def _evaluate_group_candidates(col, original_series, group_key, evaluator=None):
            """Recorre las variantes del grupo (ej. 'imputation') y evalúa cada una."""
            candidates = {}
            # Recorrer todas las variantes registradas para el tipo de la columna.
            for variant in self.technique_variants.get(self.get_type_group(inferred_type), []):
                if group_key not in variant["name"]:
                    continue
                cand_series = original_series.copy()
                cand_series, action_desc = variant["func"](cand_series)
                if group_key == "normalize":
                    perf = evaluate_normalization(cand_series, variant["name"])
                else:
                    perf = evaluator(original_series, cand_series, inferred_type)

                candidates[variant["name"]] = (cand_series, perf)
                logger.info("Columna %s: variante %s aplicada, desempeño: %s", col, variant["name"], perf)
            return candidates

        def _apply_best_candidate(col, quality_score, candidates, selector, variant_record_key):
            """Selecciona la mejor variante usando la función selector y la aplica si mejora el quality_score."""
            if variant_record_key == 'variants_normalization' or variant_record_key == 'variants_outlier':
                best_variant, metrics = selector(candidates)
            elif variant_record_key == 'variants_bias' or variant_record_key == 'variants_imputation':
                best_variant, metrics = selector(candidates, inferred_type)
            else:
                logger.error("Tipo de variante no soportado: %s", variant_record_key)
                return

            if best_variant:
                temp_df = current_df.copy()
                temp_df[col] = candidates[best_variant][0]
                temp_report = QualityReportEngine(temp_df).generate_report()
                new_qs = temp_report.get(col, {}).get("quality_score", 100)
                improvement = new_qs - quality_score
                logger.info("Columna %s: mejor variante %s mejora %.2f puntos", col, best_variant, improvement)
                if improvement > self.improvement_threshold:
                    current_df[col] = candidates[best_variant][0]
                    self.knowledge_base.setdefault(col, {}).setdefault(variant_record_key, []).append(best_variant)
            return

        # Bucle iterativo principal
        for epoch in range(1, self.max_epochs + 1):
            logger.info("Iteración %d", epoch)
            # Aplicar la remediación base
            base_engine = AdvancedDataRemediationEngine(current_df, prev_report)
            current_df, _ = base_engine.remediate_all()
            
            # Recorrer cada columna para evaluar variantes
            for col in current_df.columns:
                if self.include_fields is not None and col not in self.include_fields:
                    continue
                if self.exclude_fields is not None and col in self.exclude_fields:
                    continue
            
                col_report = prev_report.get(col, {})
                quality_score = col_report.get("quality_score", 100)
                inferred_type = col_report.get("inferred_type", "unknown")
                # Solo evaluar columnas con calidad baja (por ejemplo, < 90)
                if quality_score < 90:
                    original_series = current_df[col].copy()
                    # Evaluar variantes para imputación (aquellas cuyo nombre contenga "impute")
                    candidates_impute = _evaluate_group_candidates(col, original_series, "impute",
                                                                   lambda orig, cand, t: evaluate_imputation(orig, cand, t))
                    _apply_best_candidate(col, quality_score, candidates_impute,
                                          select_best_imputation, "variants_imputation")
                    
                    # Evaluar variantes para normalización (aquellas cuyo nombre contenga "normalize")
                    candidates_norm = _evaluate_group_candidates(col, original_series, "normalize")
                    _apply_best_candidate(col, quality_score, candidates_norm,
                                          select_best_normalization, "variants_normalization")
                
                    # Evaluar variantes para manejo de atípicos (por ejemplo, "atypical" o "winsorize")
                    candidates_atypical = _evaluate_group_candidates(col, original_series, "atypical",
                                                                   lambda orig, cand, t: evaluate_outlier_handling(orig, cand))
                    _apply_best_candidate(col, quality_score, candidates_atypical,
                                          select_best_outlier_handling, "variants_outlier")
                
                    # Evaluar variantes para reducción de sesgo (bias)
                    candidates_bias = _evaluate_group_candidates(col, original_series, "bias",
                                                                   lambda orig, cand, t: evaluate_bias(orig, cand, t))
                    _apply_best_candidate(col, quality_score, candidates_bias,
                                          select_best_bias, "variants_bias")
                
            new_report = QualityReportEngine(current_df).generate_report()
            new_global_score = new_report["global"]["average_quality_score"]
            logger.info("Global Quality Score en iteración %d: %.2f", epoch, new_global_score)
            self.iteration_logs.append((f"Iteración_{epoch}", new_global_score))
            self.iteration_history.append({"epoch": f"Iteración_{epoch}", "report": new_report})
            if new_global_score - prev_global_score < self.improvement_threshold:
                logger.info("Mejora global insuficiente, deteniendo iteraciones.")
                break
            prev_global_score = new_global_score
            prev_report = new_report

        try:
            with open("iteration_history.json", 'w') as f:
                json.dump(self.iteration_history, f, indent=4)
            logger.info("Historial de iteraciones guardado en iteration_history.json")
        except Exception as e:
            logger.error("Error al guardar historial de iteraciones: %s", str(e))
        self.save_knowledge()
        return current_df, self.iteration_logs, self.knowledge_base
