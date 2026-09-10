import os
import yaml
import json
from pyspark.sql import functions as F


class SemanticEngine:
    def __init__(self, core_engine, models_dir="semantic_models"):
        """
        Initialise le moteur sémantique en se basant sur le moteur Core
        (pour hériter de la session Spark et du contexte de Sandbox).
        """
        self.core = core_engine
        self.spark = core_engine.spark
        self.models_dict = self._load_all_models(models_dir)
        print(f"🧠 [Semantic Layer] Loaded {len(self.models_dict)} models into memory.")

    def _load_all_models(self, models_dir):
        """Parcourt le dossier semantic_models/ et charge tous les YAML en mémoire."""
        # On cherche le dossier à la racine du projet (au même niveau que config.yaml)
        current_file_path = os.path.abspath(__file__)
        project_root = os.path.dirname(os.path.dirname(current_file_path))
        full_dir_path = os.path.join(project_root, models_dir)

        models_dict = {}
        if not os.path.exists(full_dir_path):
            print(f"⚠️ [Semantic Layer] Directory not found: {full_dir_path}")
            return models_dict

        for filename in os.listdir(full_dir_path):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                file_path = os.path.join(full_dir_path, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = yaml.safe_load(f)
                    if content and "models" in content:
                        for model in content["models"]:
                            models_dict[model["name"]] = model

        return models_dict

    def query(self, model_name: str, metrics: list, group_by: list = None):
        """
        Génère dynamiquement la requête PySpark à partir du modèle sémantique.

        Args:
            model_name: Le nom du modèle (ex: 'kpi_export_sales')
            metrics: Liste des métriques à calculer (ex: ['gross_revenue'])
            group_by: Liste des dimensions de regroupement (ex: ['sales_org_code'])
        """
        if model_name not in self.models_dict:
            raise ValueError(f"❌ Model '{model_name}' not found in semantic definitions.")

        model_def = self.models_dict[model_name]

        # 1. Résolution de la table source (Gère la Sandbox automatiquement via Core)
        # Ex: "gold.kpi_export_sales" -> layer="gold", table="kpi_export_sales"
        raw_table_parts = model_def["table"].split(".")
        layer = raw_table_parts[0]
        table_base = raw_table_parts[1]

        actual_schema = self.core.get_target_schema(layer)
        fqn = f"`{self.core.db}`.`{actual_schema}`.`{table_base}`"

        print(f"🔎 [Semantic Query] Targeting table: {fqn}")
        df = self.spark.table(fqn)

        # 2. Préparation des expressions PySpark
        group_exprs = []
        agg_exprs = []

        # Dictionnaires pour recherche rapide
        dim_dict = {d["name"]: d for d in model_def.get("dimensions", [])}
        met_dict = {m["name"]: m for m in model_def.get("metrics", [])}

        # 3. Construction du GROUP BY (Dimensions)
        if group_by:
            for dim_name in group_by:
                if dim_name not in dim_dict:
                    raise ValueError(f"❌ Dimension '{dim_name}' not found in model '{model_name}'.")

                dim_sql = dim_dict[dim_name]["sql"]
                # On utilise F.expr au cas où la dimension utilise du SQL complexe
                group_exprs.append(F.expr(dim_sql).alias(dim_name))

        # 4. Construction des AGGREGATIONS (Métriques + Filtres)
        for met_name in metrics:
            if met_name not in met_dict:
                raise ValueError(f"❌ Metric '{met_name}' not found in model '{model_name}'.")

            met_def = met_dict[met_name]
            base_col_expr = F.expr(met_def["sql"])

            # Application des filtres métier spécifiques à cette métrique
            if "filters" in met_def and met_def["filters"]:
                # On combine tous les filtres de la métrique avec un AND
                filter_cond = " AND ".join([f["sql"] for f in met_def["filters"]])
                # Astuce PySpark : F.when(condition, valeur).otherwise(None)
                base_col_expr = F.when(F.expr(filter_cond), base_col_expr).otherwise(F.lit(None))

            # Application de la fonction d'agrégation (sum, count_distinct, etc.)
            agg_type = met_def["type"].lower()
            if agg_type == "sum":
                agg_expr = F.sum(base_col_expr)
            elif agg_type == "count_distinct":
                agg_expr = F.countDistinct(base_col_expr)
            else:
                raise NotImplementedError(f"❌ Aggregation '{agg_type}' not yet supported.")

            agg_exprs.append(agg_expr.alias(met_name))

        # 5. Exécution finale
        if group_exprs:
            return df.groupBy(*group_exprs).agg(*agg_exprs)
        else:
            return df.agg(*agg_exprs)

    def get_llm_context(self, model_name: str) -> str:
        """
        Génère un dictionnaire épuré (format texte JSON) pour le prompt du LLM.
        Masque toute la complexité SQL/PySpark et ne garde que la sémantique métier.
        """
        if model_name not in self.models_dict:
            raise ValueError(f"❌ Model '{model_name}' not found in semantic definitions.")

        model_def = self.models_dict[model_name]

        # On construit un objet strict et propre pour le LLM
        llm_context = {
            "dataset_name": model_name,
            "description": model_def.get("description", ""),
            "available_dimensions": [],
            "available_metrics": []
        }

        # On extrait les dimensions en cachant le champ 'sql'
        for dim in model_def.get("dimensions", []):
            llm_context["available_dimensions"].append({
                "name": dim["name"],
                "type": dim["type"],
                "description": dim.get("description", "")
            })

        # On extrait les métriques en cachant les champs 'sql' et 'filters'
        for met in model_def.get("metrics", []):
            llm_context["available_metrics"].append({
                "name": met["name"],
                "description": met.get("description", "")
            })

        # On retourne une chaîne JSON formatée, parfaite pour être injectée dans un prompt
        return json.dumps(llm_context, indent=2, ensure_ascii=False)


