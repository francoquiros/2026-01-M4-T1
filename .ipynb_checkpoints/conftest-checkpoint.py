import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope = "module")

def spark_session():
    """A fixture to create a Spark Context to reuse across tests."""
    session = SparkSession.builder.appName('Solucion_Tarea_1').master('local').getOrCreate()

    yield session

    session.stop()