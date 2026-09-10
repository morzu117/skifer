import os
import sys

import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Force les workers et le driver Spark à utiliser le même interpréteur Python
# que celui qui exécute les tests (celui du venv). Sans cela, le worker lance
# le `python3` du PATH système, ce qui provoque PYTHON_VERSION_MISMATCH si la
# version système diffère de celle du venv.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    """
    Crée une session Spark locale pour les tests, configurée pour Delta Lake.
    
    Cette fixture utilise la méthode recommandée par le package `delta-spark`
    pour configurer la session Spark. `configure_spark_with_delta_pip`
    s'assure que la bonne version des JARs Delta, déjà installée localement
    par pip, est utilisée. C'est plus robuste que de dépendre du téléchargement
    via `spark.jars.packages`.
    
    La configuration inclut également `spark.driver.host` pour la stabilité
    des tests sur macOS.
    """
    builder = SparkSession.builder \
        .appName("skifer-tests") \
        .master("local[2]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # Utilise la fonction helper de delta-spark pour injecter la configuration du JAR
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    yield spark
    spark.stop()
