# Plan 45 — Exécution sémantique intégrable dans Board

Date : 3 novembre 2026. Branche : `feat/board-semantic-runtime`. Propriétaire : équipe Skifer/Board.

## Objectif et périmètre

Board a choisi Skifer en bibliothèque : Board vérifie l'identité, configure ses connexions et exécute le SQL produit par Skifer sans le modifier. Ce plan couvre les attendus SK-01 (dialecte cible et runtime sans Spark), SK-02 réduit (refus typé) et SK-08 (RLS dans le planner). Il n'ajoute ni API distante ni vérificateur OIDC à Skifer. Les routes de Board restent fermées tant que le contrat complet et ses tests ne sont pas livrés.

## Décisions

1. `SemanticEngine` accepte un backend d'exécution injecté publiquement, un catalogue, un environnement et une configuration, sans créer de `SkiferEngine` ni de session Spark. Le contrat minimal exécute le SQL et retourne les données avec métadonnées ; le chemin historique `SemanticEngine(core_engine)` reste compatible. Un backend absent refuse l'exécution avant tout accès aux données. Le service qui matérialise des lignes ne présume pas un DataFrame PySpark si une exécution sans Spark est demandée.
2. `QueryResolver` accepte explicitement `databricks` ou `duckdb` comme dialecte cible. Il compile le plan déterministe et rend un SQL prêt à exécuter. Un dialecte inconnu est refusé. Les identifiants, littéraux, filtres de base, dates, métriques et jointures sont vérifiés sur les deux dialectes. Aucun SQL libre provenant de la requête n'est accepté.
3. `sql_hash` couvre le SQL réellement transmis au backend, et l'evidence donne le dialecte. Une même demande logique peut donc avoir deux hashs différents ; la comparaison ou la clé de cache doit inclure le dialecte.
4. La certification existante produit déjà `SemanticAccessDenied` avec décision, raisons et action recommandée. Ce type est la frontière publique du refus SK-02 sous l'option bibliothèque ; `DENY` et `REQUIRE_HUMAN` ne déclenchent aucune exécution. Sa sérialisation côté Board est hors de ce dépôt.
5. Une politique de lignes déclarée et validée sur chaque modèle lie une colonne SQL sûre à une valeur issue exclusivement du `ConsumerContext` vérifié par l'appelant. Le planner construit les restrictions pour **tous** les modèles lus, avant compilation. Une identité ou un attribut requis absent refuse fermé ; aucun rôle d'administrateur Board ne contourne cette politique. Les filtres d'exploration du lecteur ne peuvent ni remplacer ni retirer la restriction. Le filtre de base est également attesté.
6. La preuve inclut chaque restriction de sécurité avec `origin: policy`, sa colonne et son opérateur ; sa valeur est masquée par défaut. Les attributs d'identité bruts n'entrent ni dans le texte public de la preuve ni dans les traces. Le SQL exécuté porte la valeur, et son hash change avec celle-ci.

## Points de livraison — un commit par point

- **45.1 — Runtime public et refus typé.** Instanciation sans Spark, backend injecté, refus de certification intact, essais des modes sans backend et des résultats non Spark. Mise à jour de la documentation et du changelog.
- **45.2 — Dialectes SQL.** `databricks` et `duckdb` dans le résolveur et le moteur ; preuves de parité des lignes et de `sql_hash`/dialecte. Tests de littéraux et de jointures, y compris refus des entrées invalides.
- **45.3 — RLS et preuve.** Schéma de politique, validation, planner, compilation, filtres de base et evidence ; tests à deux lecteurs, filtre utilisateur retiré, administrateur, identité absente et modèles joints. Aucun résultat n'est servi si la politique ne peut être appliquée.
- **45.4 — Contrat Board.** Exemples de l'API publique, test d'intégration DuckDB et contrat de refus, documentation de version cible. La publication PyPI suit la PR fusionnée et les contrôles verts.

## Acceptation

`pytest tests/ -x --tb=short`, `ruff check src/` et `mkdocs build --strict` passent. Les tests prouvent que deux lecteurs voient des lignes distinctes sur le même backend, qu'un administrateur ne voit pas plus qu'un lecteur, qu'un filtre utilisateur supprimé ne retire pas la RLS, qu'un refus n'exécute aucun SQL et que les deux dialectes donnent le même résultat logique. Board pourra alors épingler la version PyPI publiée et tester son adaptateur avant d'ouvrir ses routes d'exécution.

## État de réalisation

- 45.1 : constructeur `SemanticEngine.from_backend()` et lignes bornées sans DataFrame Spark ; tests ciblés et suite complète en cours de validation.
