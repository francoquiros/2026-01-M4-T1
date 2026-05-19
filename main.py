# Imports

from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType, FloatType, DateType)
import sys
from functions import join_dfs, aggregate, top_n

# Inputs para crear los dataframes

ciclistas = sys.argv[1]
rutas = sys.argv[2]
actividad = sys.argv[3]

# Creaciónl de la sesión de Spark

SparkSession.builder


# Se construyen los conjuntos de datos



# Función para establecer el JOIN



# Función para calcular



# Función para extraer el top