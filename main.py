# Paquetes y extensiones necesarios
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, FloatType, DateType
from functions import join_cyclist_activity_route, aggregate_kms_per_cyclist_and_date, top_n_cyclists

# Configuración y creación de la Sesión de Spark
spark = SparkSession.builder.appName("Solucion_Tarea_1").getOrCreate()

# Se obtienen los paths de los archivos csv, este orden:
# ciclistas.csv   rutas.csv   actividad.csv
# Estos se espera que estén en la misma carpeta que este archivo .py
# por lo que solo es necesario que se especifique el nombre del csv
cyclists_file = sys.argv[1]
routes_file = sys.argv[2]
activities_file = sys.argv[3]

# Se construyen los esquemas de los dataframes
schema_cyclist = StructType([
    StructField("cedula", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("provincia", StringType(), True),
])

schema_route = StructType([
    StructField("cod_ruta", IntegerType(), True),
    StructField("nombre_ruta", StringType(), True),
    StructField("km", FloatType(), True),
])

schema_activity = StructType([
    StructField("cod_ruta", IntegerType(), True),
    StructField("cedula", IntegerType(), True),
    StructField("fecha", DateType(), True),
])

# Se aplican los esquemas para construir los dataframes
df_cyclists = spark.read.csv(cyclists_file, schema=schema_cyclist, header=False, nullValue="NA")
df_routes = spark.read.csv(routes_file, schema=schema_route, header=False, nullValue="NA")
df_activities = spark.read.csv(activities_file, schema=schema_activity, header=False, nullValue="NA")

# Función para ejecutar el join entre los dataframes
joined_df = join_cyclist_activity_route(df_cyclists, df_activities, df_routes)
# Se muestran las primeras filas del resultado
print("UNION DE LOS DATAFRAMES")
joined_df.show()

# Función para calcular las agregaciones parciales
aggregated_df = aggregate_kms_per_cyclist_and_date(joined_df)
# Se muestran las primeras filas del resultado
print("DATAFRAME INTERMEDIO DE DATOS AGREGADOS")
aggregated_df.show()

# Función para extraer el top N = 5 de ciclistas
top_5_per_province = top_n_cyclists(aggregated_df, 5)
# Se muestran las primeras filas del resultado
print("DATAFRAME CON RESULTADOS FINALES (TOP N = 5 POR PROVINCIA)")
top_5_per_province.show()

# Se detiene la sesión de Spark
spark.stop()
