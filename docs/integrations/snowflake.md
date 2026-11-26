# Snowflake — connecteur v0.1

**Aucune opération n’est `Supported` sur Snowflake tant que la parité n’a pas été exécutée
contre un compte réel.** Le connecteur a été testé sur faux curseur uniquement : **jamais
exécuté contre un vrai compte**. Le critère du Plan 46.6 exigeant un run d’intégration réel
reste non satisfait, en attente d’un compte.

Le paquet séparé [skifer-snowflake](https://github.com/morzu117/skifer-snowflake) implémente
les trois contrats étroits du cœur : exécution SQL, catalogue et publication. Le cœur
compile et rend le SQL ; le connecteur utilise une connexion DB-API injectée et retourne
le curseur natif, sans DataFrame universel ni dispatch d’opérations par plateforme.

## Installation

Python 3.10 ou supérieur, `skifer[dialects]>=4.2.0`, `snowflake-connector-python>=3.0` :

```bash
pip install skifer-snowflake
```

Avant la release 4.2, installer les sources du Plan 46 :

```bash
cd /chemin/vers/skifer-snowflake
/chemin/vers/skifer/.venv/bin/python -m venv .venv
.venv/bin/pip install -e '/chemin/vers/skifer[dialects]' pytest ruff
.venv/bin/pip install -e . --no-deps
```

Le cœur local reste estampillé 4.1.0 : `--no-deps` contourne uniquement le floor du connecteur,
après installation des dépendances nécessaires aux tests. Le driver Snowflake reste une
dépendance déclarée mais n’est pas installé dans ce venv de tests. Pour un vrai run,
l’installer séparément. La CI prend temporairement le cœur depuis `main` et ne peut être
verte avant que le Plan 46 y soit fusionné ; retour à PyPI prévu à la release 4.2.

## Connexion injectée et casse exacte

`SnowflakeConnector(connection, *, database, schema)` reçoit une connexion déjà ouverte
par l’appelant. Il ne lit aucun fichier de configuration, aucune variable d’environnement,
ne construit aucun credential et ne ferme jamais cette connexion.

Le rendu Snowflake cite **toujours** les identifiants de tables, colonnes et alias, en
préservant la casse de l’auteur. Une table créée sans guillemets sous le nom `orders`
s’appelle `ORDERS` dans Snowflake : écrire `ORDERS` dans le YAML. Cette règle vaut pour
les pipelines et les modèles sémantiques, expressions comprises. Les guillemets internes
aux composants d’un nom sont doublés ; fournir des noms littéraux sans guillemets SQL
englobants. Un point sépare deux composants, il ne peut pas faire partie d’un composant.

**Casse des identifiants.** Limite connue : Spark est insensible à la casse des
identifiants, tandis que le rendu Snowflake les cite à casse exacte. Un alias `Total`
référencé comme `total` ailleurs échoue sur Snowflake (ou lie une autre colonne si
les deux existent) ; il en va de même pour `Country` projeté et `country` dans le
`GROUP BY`. La casse doit donc être cohérente dans tout le YAML ciblant Snowflake ;
aucune normalisation automatique n’est appliquée. `current_date` et
`current_timestamp` restent des fonctions, mais `localtimestamp` sans parenthèses
est rendu comme la colonne `"localtimestamp"`. Ces rendus sont épinglés par des tests
locaux ; ils n’ont pas été exécutés sur un compte Snowflake.

Exemple illustratif, non exécuté contre Snowflake :

```python
from contextlib import closing
import snowflake.connector
from skifer.core.ir import parse_to_ir
from skifer.core.schema_loader import parse_schema
from skifer_snowflake import SnowflakeConnector

# Les credentials sont fournis par l’application appelante.
with closing(snowflake.connector.connect(
    account=account, user=user, password=password, warehouse=warehouse,
    database="MY_DB", schema="PUBLIC", autocommit=True,
    session_parameters={"QUOTED_IDENTIFIERS_IGNORE_CASE": False},
)) as connection:
    connector = SnowflakeConnector(connection, database="MY_DB", schema="PUBLIC")
    parsed = parse_to_ir(parse_schema('''
        tables: [{name: ORDERS, alias: ord}]
        select_final: [[COUNTRY, country, [upper]]]
    '''))
    print(connector.report(parsed))
    print(connector.compile(parsed))
    result = connector.execute_schema(parsed)
    with closing(result.dataframe) as cursor:
        print(result.statement_id, cursor.fetchall())
```

Utiliser les paramètres liés par défaut `pyformat` (`%s`), une connexion par thread et
`QUOTED_IDENTIFIERS_IGNORE_CASE=FALSE`. Le catalogue interroge `INFORMATION_SCHEMA` avec
valeurs liées et comparaisons exactes. `columns()` d’une relation absente lève `LookupError`.
Un nom à une partie devient `database.schema.name`, à deux parties `database.s.n`,
à trois parties reste inchangé. Aucun schéma n’est créé implicitement.

`execute_sql(sql)` exécute le SQL brut fourni tel quel ; sa citation relève de l’appelant.
`execute_sql_with_metadata` renvoie un `ExecutionResult` dont `dataframe` est le curseur
exécuté, `statement_id` son `sfqid` (`None` si absent), et les métadonnées indiquent
`snowflake`. L’appelant consomme et ferme ce curseur, via `fetchall()` ou
`fetch_pandas_all()` si les dépendances pandas du driver sont installées.

## Publication et sémantique

Les publications exigent un seul SELECT, validé par sqlglot avant émission ; les CTE sont
admises, les statements multiples, DML, SELECT INTO et unions à la racine sont refusés.
`overwrite` utilise `CREATE OR REPLACE TABLE ... AS ...`, les vues
`CREATE OR REPLACE VIEW ... AS ...`. Chaque publication refuse une transaction
déjà ouverte avant toute lecture du catalogue ou écriture : commiter ou annuler d’abord.
Les DDL Snowflake auto-commitent : aucun rollback ni garantie transactionnelle
supplémentaire n’est promis pour ces publications.

`append` exige une table de base existante et les mêmes noms exacts de colonnes, sans doublons,
manquants ni colonnes supplémentaires. Le SELECT est décrit par `cursor.describe()` sans être
exécuté ; l’INSERT nomme les colonnes dans l’ordre de la table. **Les types ne sont pas
comparés** : la correspondance entre les codes de types du driver et `DATA_TYPE` du catalogue
n’a pas été vérifiée sur un compte. Les conversions restent celles du serveur.

Après la garde `CURRENT_TRANSACTION()` et le préflight, l’append émet
`BEGIN` / `INSERT` / `COMMIT`, ou `ROLLBACK` sur erreur, en conservant l’exception d’origine
si le rollback échoue. Une erreur réseau au commit laisse l’issue inconnue ; relancer
peut dupliquer les lignes, l’appelant doit vérifier. Il ne rejoint pas une transaction
de l’appelant comme DuckDB.
Utiliser une connexion dédiée avec autocommit activé. Le protocole est vérifié sur faux
curseur ; ses effets serveur sont encore non vérifiés.

`semantic_engine(models_dir, connector, *, catalog=None, environment="DEV", config=None,
certification_store=None)` délègue à `SemanticEngine.from_backend(..., dialect="snowflake")`.
Fournir un chemin absolu et des tables sémantiques entièrement qualifiées, ou préciser
`catalog` pour les tables à deux parties. Les valeurs par défaut du connecteur ne réécrivent
pas les modèles. Le catalogue charge au démarrage, les modèles à la demande ; la certification
et les preuves restent gérées par le cœur.

## Capacités et preuves

La matrice du cœur est l’unique source de capacités : **51 `Conditional`, 6 `Refused`,
0 `Supported`**. Le motif par défaut est `parity not executed against a Snowflake account`.
Les conditions particulières de `cast`, `split` et `to_date` restent déclarées par le cœur.

| Clés refusées | Motif |
| --- | --- |
| `filter:sql`, `op:expr` | Les fragments bruts sont écrits en Spark SQL et ne sont pas rendus pour une autre cible. |
| `agg:first`, `agg:last` | Non déterministes sans ordre, pas d’équivalent portable déclaré. |
| `join:left_semi`, `join:left_anti` | Pas de syntaxe SEMI/ANTI JOIN pour cette cible. |

Ces refus remontent comme `SqlCompilationError` avant exécution. `report(parsed)` expose
les conditions ; pouvoir compiler n’établit pas une parité de résultat. Les règles Python,
le streaming, les upserts et `PublicationCoordinator` restent hors périmètre.

Le connecteur fige **23 cas : 20 SQL compilés et 3 refus**. Les tests comparent le SQL à
l’octet, le reparsent en Snowflake avec sqlglot et vérifient la citation des identifiants
dans l’AST. Ils prouvent la stabilité et la validité syntaxique selon sqlglot, pas
l’acceptation par le serveur ni l’égalité avec les résultats Spark.

Le test d’intégration optionnel est collecté et sauté sans
`SKIFER_SNOWFLAKE_INTEGRATION=1` et les variables de connexion. Il crée des tables temporaires
aux noms UUID dans un schéma jetable créé puis supprimé en `finally`. Il couvre aussi
le catalogue, la publication, les transactions et les métadonnées, rejoue les 20 cas
compilables et compare aux lignes
Spark figées, nombres quantifiés à `1e-9`, dates ISO et lignes comme multi-ensemble.
**Ce test n’a jamais été exécuté contre un compte réel.** Les variables et hypothèses
non vérifiées sont documentées dans le
[README du connecteur](https://github.com/morzu117/skifer-snowflake#intégration-optionnelle--jamais-exécutée).
