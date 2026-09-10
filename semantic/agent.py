import json
import os


class GenBIAgent:
    def __init__(self, semantic_engine, default_model="kpi_export_sales"):
        """
        Initialise l'agent GenBI avec le moteur sémantique.
        """
        self.semantic = semantic_engine
        self.default_model = default_model
        # On pré-charge le contexte propre (le dictionnaire métier) pour le LLM
        self.context = self.semantic.get_llm_context(self.default_model)

    def ask(self, question: str):
        """
        Prend une question en langage naturel, interroge l'API OpenAI,
        et retourne le DataFrame PySpark calculé.
        """
        print(f"👤 [User] Question : '{question}'")

        # 1. Le Prompt Système : On cadenasse l'IA pour éviter les hallucinations
        system_prompt = f"""You are an expert in Data Anlytics.
        Your role here is just to translate questions coming from users in JSON request.
        You have not access to the database. The unique and only thing you need to use is the following semantic dictionary :
        {self.context}
        
        Rules to strictly apply :
        - You MUST ANSWER with a JSON object. no speech before or after the json object
        - the JSON object MUST HAVE the EXACT following structure : 
        {{
            "metrics": ["metric_name_1"],
            "group_by": ["dimension_name_1"]
        }}
        - Deduce metrics and dimensions regarding the question meaning
        - If the question has no mention 
        - If the question does not mention a grouping dimension, return an empty list for "group_by".
        """

        # 2. Appel au LLM
        print(f"🤖 [Agent] Réflexion en cours...")
        llm_response_text = self._call_llm(system_prompt, question)

        # 3. Nettoyage et Parsing du JSON
        if not llm_response_text:
            return None

        try:
            # Les LLMs aiment bien rajouter ```json au début de leurs réponses, on nettoie ça
            clean_json = llm_response_text.replace('```json', '').replace('```', '').strip()
            query_params = json.loads(clean_json)
            print(f"🧠 [Agent] Traduction réussie : {query_params}")
        except Exception as e:
            print(f"❌ [Agent] Erreur de parsing JSON. L'IA a désobéi. Réponse brute : \n{llm_response_text}")
            return None

        # 4. Exécution sur le cluster Spark via votre SemanticEngine
        print(f"🚀 [Agent] Exécution de la requête sur Databricks...")
        try:
            df_result = self.semantic.query(
                model_name=self.default_model,
                metrics=query_params.get("metrics", []),
                group_by=query_params.get("group_by", [])
            )
            return df_result
        except Exception as e:
            print(f"❌ [Agent] Erreur lors de l'exécution PySpark : {e}")
            return None

    def _call_llm(self, system_prompt: str, user_question: str) -> str:
        """
        Fait l'appel réseau vers l'API OpenAI.
        """
        try:
            import openai
        except ImportError:
            print("❌ [Agent] La librairie 'openai' n'est pas installée. Faites un `pip install openai`.")
            return None

        # On récupère la clé depuis les variables d'environnement (vide par défaut pour l'instant)
        api_key = os.environ.get("OPENAI_API_KEY", "")

        if not api_key:
            print("⚠️ [Agent] OPENAI_API_KEY manquante. Simulation de réponse JSON pour le test...")
            # Simulation en attendant votre clé !
            return '{"metrics": ["gross_revenue"], "group_by": ["platform"]}'

        try:
            client = openai.OpenAI(api_key=api_key)

            response = client.chat.completions.create(
                model="gpt-4o",  # Modèle standard actuel
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_question}
                ],
                # TEMPÉRATURE À 0 : C'est le secret ! On veut une traduction logique, pas de la créativité.
                temperature=0.0
            )
            return response.choices[0].message.content

        except Exception as e:
            print(f"❌ [Agent] Erreur de communication avec l'API OpenAI : {e}")
            return None