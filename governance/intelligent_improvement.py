import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

class IntelligentImprovementEngine:
    def __init__(self, quality_metrics, policy, historical_data=None):
        self.quality_metrics = quality_metrics
        self.policy = policy
        self.historical_data = historical_data
        self.model = None
        if historical_data is not None:
            self.train_model(historical_data)

    def train_model(self, historical_data):
        if isinstance(historical_data, list):
            historical_df = pd.DataFrame(historical_data)
        elif isinstance(historical_data, pd.DataFrame):
            historical_df = historical_data.copy()
        else:
            raise ValueError("historical_data must be a list of dictionaries or a DataFrame")

        all_keys = set()
        for improvements in historical_df['field_improvements']:
            all_keys.update(improvements.keys())
        all_keys = list(all_keys)
        X = []
        y = []
        for idx, row in historical_df.iterrows():
            feat = [row['field_improvements'].get(key, 0) for key in all_keys]
            X.append(feat)
            y.append(row['quality_score_improvement'])
        X = np.array(X)
        y = np.array(y)

        self.model = LinearRegression()
        self.model.fit(X, y)
        self.feature_keys = all_keys

    def compute_field_quality_score(self, field, metrics):
        score = 100
        if not metrics.get("type_match", False):
            score -= 20
        score -= metrics.get("null_percentage", 0) * 0.5
        if field.get("type") in ["integer", "float"]:
            score -= metrics.get("outlier_percentage", 0) * 0.5
            skewness = metrics.get("skewness", 0)
            if skewness is not None and skewness > 1:
                score -= (skewness - 1) * 10
        if field.get("type") == "datetime":
            temporal_anomaly = metrics.get("temporal_anomaly") or 0
            score -= temporal_anomaly * 10
        if field.get("type") == "string":
            cardinality_ratio = metrics.get("cardinality_ratio") or 0
            if cardinality_ratio > 0.8:
                score -= (cardinality_ratio - 0.8) * 50
        score -= metrics.get("duplicate_percentage", 0) * 0.2
        if metrics.get("security_compliant") is False:
            score -= 15
        return max(score, 0)

    def compute_improvement_potential(self, field, metrics):
        current_score = self.compute_field_quality_score(field, metrics)
        potential = {}
        if not metrics.get("type_match", True):
            potential["type_match"] = min(20, 100 - current_score)
        null_penalty = metrics.get("null_percentage", 0) * 0.5
        potential["null_percentage"] = min(null_penalty, 100 - current_score)
        if field.get("type") in ["integer", "float"]:
            outlier_penalty = metrics.get("outlier_percentage", 0) * 0.5
            potential["outlier_percentage"] = min(outlier_penalty, 100 - current_score)
            skewness = metrics.get("skewness", 0)
            if skewness is not None and skewness > 1:
                potential["skewness"] = min((skewness - 1) * 10, 100 - current_score)
        if field.get("type") == "datetime":
            temporal_anomaly = metrics.get("temporal_anomaly") or 0
            temporal_penalty = temporal_anomaly * 10
            potential["temporal_anomaly"] = min(temporal_penalty, 100 - current_score)
        if field.get("type") == "string":
            cardinality = metrics.get("cardinality_ratio") or 0
            if cardinality > 0.8:
                cardinality_penalty = (cardinality - 0.8) * 50
                potential["cardinality_ratio"] = min(cardinality_penalty, 100 - current_score)
        duplicate_penalty = metrics.get("duplicate_percentage", 0) * 0.2
        potential["duplicate_percentage"] = min(duplicate_penalty, 100 - current_score)
        if metrics.get("security_compliant") is False:
            potential["security_compliant"] = min(15, 100 - current_score)
        return potential

    def generate_improvement_recommendations(self):
        field_recommendations = {}
        for field in self.policy.get("fields", []):
            field_name = field.get("field_name")
            if field_name not in self.quality_metrics or self.quality_metrics[field_name].get("status") == "missing":
                field_recommendations[field_name] = ["El campo está ausente; su inclusión es prioritaria."]
                continue
            metrics = self.quality_metrics[field_name]
            potential = self.compute_improvement_potential(field, metrics)
            recommendations = []
            sorted_potentials = sorted(
                [(k, v) for k, v in potential.items() if k != "predicted_improvement"],
                key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0,
                reverse=True
            )
            for metric, pot in sorted_potentials:
                if pot > 0:
                    recommendations.append(f"Optimizar '{metric}' podría incrementar el score en hasta {pot:.1f} puntos.")
            if self.model is not None:
                feature_vector = np.array([potential.get(key, 0) for key in self.feature_keys]).reshape(1, -1)
                predicted = self.model.predict(feature_vector)[0]
                recommendations.insert(0, f"Correcciones en este campo podrían incrementar el score global en aproximadamente {predicted:.1f} puntos (predicción).")
            field_recommendations[field_name] = recommendations

        global_recommendations = []
        global_metrics = self.quality_metrics.get("global", {})
        avg_score = global_metrics.get("average_quality_score", 100)
        if avg_score < 80:
            global_recommendations.append("La calidad global es baja; se recomienda una revisión integral de la ingesta.")
        if "drift_percentage" in global_metrics:
            drift = global_metrics["drift_percentage"]
            if abs(drift) > 10:
                global_recommendations.append(f"Se detectó un drift global de {drift:.1f}%; revisar la evolución histórica.")

        relational_results = self.check_relational_rules()
        for rule, valid in relational_results.items():
            if not valid:
                global_recommendations.append(f"La regla relacional '{rule}' no se cumple; revisar relaciones entre campos.")

        return {"global": global_recommendations, "fields": field_recommendations}

    def check_relational_rules(self):
        results = {}
        if "relational_rules" in self.policy:
            for rule in self.policy["relational_rules"]:
                expr = rule.get("rule")
                fields = rule.get("fields", [])
                if expr and len(fields) == 2:
                    # Placeholder para validación real
                    results[f"{fields[0]} {expr} {fields[1]}"] = True
        return results

    def update_model(self, new_historical_data):
        if self.historical_data is None:
            self.historical_data = new_historical_data
        else:
            if isinstance(self.historical_data, list):
                self.historical_data.extend(new_historical_data if isinstance(new_historical_data, list) else [new_historical_data])
            elif isinstance(self.historical_data, pd.DataFrame):
                new_df = pd.DataFrame(new_historical_data) if isinstance(new_historical_data, list) else new_historical_data
                self.historical_data = pd.concat([self.historical_data, new_df], ignore_index=True)
        self.train_model(self.historical_data)

    def generate_visualization_data(self):
        fields = []
        current_scores = []
        potentials = []
        for field in self.policy.get("fields", []):
            field_name = field.get("field_name")
            if field_name in self.quality_metrics and "field_quality_score" in self.quality_metrics[field_name]:
                fields.append(field_name)
                metrics = self.quality_metrics[field_name]
                current_score = self.compute_field_quality_score(field, metrics)
                pot_dict = self.compute_improvement_potential(field, metrics)
                total_potential = sum(pot_dict.values())
                total_potential = min(total_potential, 100 - current_score)
                current_scores.append(current_score)
                potentials.append(total_potential)
        potential_scores = [cs + pot for cs, pot in zip(current_scores, potentials)]
        return {
            "fields": fields,
            "current_scores": current_scores,
            "potential_scores": potential_scores
        }
