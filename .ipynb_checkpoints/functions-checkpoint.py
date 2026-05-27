import sys
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, FloatType, DateType
from pyspark.sql.functions import to_date, col, lit, desc, sum, mean, round, dense_rank, row_number

# Función para unir los tres dataframes: ciclistas, actividades y rutas
def join_cyclist_activity_route(df_cyclists, df_activities, df_routes):

    # PARTE 1: Actividad y Ruta
    # Debido a que el dataframe de "actividad" siempre debe tener un código de ruta, y no interesa una ruta que
    # no haya sido transitada en alguna actividad, el inner join es suficiente entre estos dos dataframes
    df_activities_routes = df_activities.join(df_routes, on="cod_ruta", how="inner")

    # PARTE 2: Ciclistas y Actividad/Ruta
    # Entre estos dos dataframes interesa preservar todos los ciclistas, hayan realizado alguna actividad o no.
    # Por otro lado, es posible que el mismo ciclista haya realizado la misma ruta más de una vez en el mismo día.
    # Por lo tanto, conviene primero ejecutar un right join entre ciclistas/actividad para preservar todas las actividades
    df_activities_cyclists = df_cyclists.join(df_activities_routes, on="cedula", how="right")

    # PARTE 3: Ciclistas nuevos
    # Se incluyen ahora los ciclistas que no han realizado ninguna actividad, esto por medio de un left anti join
    # entre el dataframe de ciclistas y el dataframe obtenido de la primera parte: Actividad/Ruta
    df_new_cyclists = df_cyclists.join(df_activities_routes, on="cedula", how="left_anti")

    # PARTE 4: Unión entre los dataframes de las partes 2 y 3
    # Se construye el dataframe completo al unir verticalmente los dataframes de todas las actividades con los datos
    # completos y los ciclistas nuevos. Primero, se debe agregar al dataframe de ciclistas nuevos, las columnas faltantes.
    df_new_cyclists = df_new_cyclists.withColumn("cod_ruta", lit(None).cast(IntegerType()))
    df_new_cyclists = df_new_cyclists.withColumn("fecha", lit(None).cast(DateType()))
    df_new_cyclists = df_new_cyclists.withColumn("nombre_ruta", lit(None).cast(StringType()))
    df_new_cyclists = df_new_cyclists.withColumn("km", lit(None).cast(FloatType()))
    # Se hace el join para obtener el dataframe completo
    df_complete = df_activities_cyclists.unionByName(df_new_cyclists)

    # Se retorna el dataframe completo
    return df_complete

# Función para calcular la suma de los kilómetros por persona, provincia y día
def aggregate_kms_per_cyclist_and_date(df_cyclist_activity_route):

    # Se agrupan los datos por cédula, nombre, provincia y fecha y luego
    # se calcula la suma de la columna "km" (distancia)
    df_result = df_cyclist_activity_route.groupBy("cedula", "nombre", "provincia", "fecha").agg(sum("km").alias("km_sum"))
    df_result = df_result.withColumn("km", round(df_result.km_sum, 2))
    df_result = df_result.select("cedula", "nombre", "provincia", "fecha", "km")

    return df_result

# Función que obtiene el top N de ciclistas en cada provincia,
# según el total de kilómetros recorridos y según el promedio
# diario de kilómetros recorridos
def top_n_cyclists(df_partial_aggregate, n):

    df_partial_aggregate_clean = df_partial_aggregate.dropna()

    df_result = df_partial_aggregate_clean.groupBy("provincia", "cedula", "nombre")\
                                          .agg(sum("km").alias("km_total_sum"),\
                                               mean("km").alias("km_prom_diario_avg"))

    df_result = df_result.withColumn("km_total", round(df_result.km_total_sum, 2))\
                         .withColumn("km_prom_diario", round(df_result.km_prom_diario_avg, 2))

    window_spec = Window.partitionBy("provincia")\
                        .orderBy(col("km_total").desc(), col("km_prom_diario").desc())
    
    df_result = df_result.withColumn("rank", dense_rank().over(window_spec))\
                         .filter(col("rank") <= n)\
                         .select("rank", "provincia", "cedula", "nombre", "km_total", "km_prom_diario")

    return df_result