#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Importación de paquetes y funciones necesarias
import sys
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, FloatType, DateType
from pyspark.sql.functions import to_date, col, lit, desc, sum, mean, round, dense_rank, row_number
from pyspark.testing import assertDataFrameEqual

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# Importación de las funciones desarrolladas
from functions import join_cyclist_activity_route, aggregate_kms_per_cyclist_and_date, top_n_cyclists

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# SCHEMAS TEST 1

schema_cyclist = StructType([
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True)
])

schema_activity = StructType([
            StructField("cod_ruta", IntegerType(), True),
            StructField("cedula", IntegerType(), True),
            StructField("fecha", StringType(), True)
])

schema_route = StructType([
            StructField("cod_ruta", IntegerType(), True),
            StructField("nombre_ruta", StringType(), True),
            StructField("km", FloatType(), True)
])

schema_correct_1 = StructType([
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("cod_ruta", IntegerType(), True),
            StructField("fecha", StringType(), True),
            StructField("nombre_ruta", StringType(), True),
            StructField("km", FloatType(), True)
])

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# TEST 1: Prueba de la función del join de dataframes

def test_join_pass_1(spark_session):
    
    print("PRUEBA UNITARIA 1.1: Prueba 1 del join de los tres dataframes")
    
    ciclistas_1 = [(207580425, 'Alonso Arguedas', 'Heredia'),
                   (209190484, 'Elias Herrera', 'Alajuela'),
                   (408470591, 'Marco Jimenez', 'Heredia'),
                   (301780416, 'Javier Hernandez', 'Cartago'),
                   (118740876, 'Gabriel Vargas', 'San José'),
                   (305840417, 'Fabián Solera', 'Cartago'),
                   (601980756, 'Josue Granados', 'Puntarenas'),
                   (705280357, 'Andrés Johnson', 'Limón'),
                   (116570249, 'Rebeca Chaves', 'Heredia')]
    ciclistas_1_df = spark_session.createDataFrame(ciclistas_1, schema = schema_cyclist)
    
    rutas_1 = [(703, 'San José de la Montaña - Sabanilla', 35.8),
               (484, 'Belén - Poás', 45.3),
               (795, 'Escazú - Cartago', 28.3),
               (932, 'Hatillo - Sabana', 10.6),
               (165, 'Paraíso - Alajuela', 60.4)]
    rutas_1_df = spark_session.createDataFrame(rutas_1, schema = schema_route)

    actividad_1 = [(484, 207580425, '2024-02-18'),
                   (703, 209190484, '2024-03-21'),
                   (795, 408470591, '2024-04-22'),
                   (932, 301780416, '2024-05-08'),
                   (165, 118740876, '2024-06-09'),
                   (795, 305840417, '2024-06-12'),
                   (484, 601980756, '2024-07-01'),
                   (165, 705280357, '2024-08-09'),
                   (703, 601980756, '2024-10-23'),
                   (484, 301780416, '2024-10-05'),
                   (795, 408470591, '2024-11-17'),
                   (484, 209190484, '2024-06-13'),
                   (484, 209190484, '2024-06-13'),
                   (932, 207580425, '2024-08-26'),
                   (795, 209190484, '2024-09-29'),
                   (703, 305840417, '2024-12-14')]
    actividad_1_df = spark_session.createDataFrame(actividad_1, schema = schema_activity)

    correct_11_df = spark_session.createDataFrame([(207580425, 'Alonso Arguedas', 'Heredia', 484, '2024-02-18', 'Belén - Poás', 45.3),
                                                   (209190484, 'Elias Herrera', 'Alajuela', 703, '2024-03-21', 'San José de la Montaña - Sabanilla', 35.8),
                                                   (408470591, 'Marco Jimenez', 'Heredia', 795, '2024-04-22', 'Escazú - Cartago', 28.3),
                                                   (301780416, 'Javier Hernandez', 'Cartago', 932, '2024-05-08', 'Hatillo - Sabana', 10.6),
                                                   (118740876, 'Gabriel Vargas', 'San José', 165, '2024-06-09', 'Paraíso - Alajuela', 60.4),
                                                   (305840417, 'Fabián Solera', 'Cartago', 795, '2024-06-12', 'Escazú - Cartago', 28.3),
                                                   (601980756, 'Josue Granados', 'Puntarenas', 484, '2024-07-01', 'Belén - Poás', 45.3),
                                                   (705280357, 'Andrés Johnson', 'Limón', 165, '2024-08-09', 'Paraíso - Alajuela', 60.4),
                                                   (601980756, 'Josue Granados', 'Puntarenas', 703, '2024-10-23', 'San José de la Montaña - Sabanilla', 35.8),
                                                   (301780416, 'Javier Hernandez', 'Cartago', 484, '2024-10-05', 'Belén - Poás', 45.3),
                                                   (408470591, 'Marco Jimenez', 'Heredia', 795, '2024-11-17', 'Escazú - Cartago', 28.3),
                                                   (209190484, 'Elias Herrera', 'Alajuela', 484, '2024-06-13', 'Belén - Poás', 45.3),
                                                   (209190484, 'Elias Herrera', 'Alajuela', 484, '2024-06-13', 'Belén - Poás', 45.3),
                                                   (207580425, 'Alonso Arguedas', 'Heredia', 932, '2024-08-26', 'Hatillo - Sabana', 10.6),
                                                   (209190484, 'Elias Herrera', 'Alajuela', 795, '2024-09-29', 'Escazú - Cartago', 28.3),
                                                   (305840417, 'Fabián Solera', 'Cartago', 703, '2024-12-14', 'San José de la Montaña - Sabanilla', 35.8),
                                                   (116570249, 'Rebeca Chaves', 'Heredia', None, None, None, None)],
                                                   schema = schema_correct_1)
    
    correct_11_df = correct_11_df.withColumn("fecha", to_date(col("fecha")))

    output_t11_df = join_cyclist_activity_route(ciclistas_1_df, actividad_1_df, rutas_1_df)

    assertDataFrameEqual(output_t11_df, correct_11_df)

def test_join_pass_2(spark_session):

    print("PRUEBA UNITARIA 1.2: Prueba 2 del join de los tres dataframes")
    
    ciclistas_2 = [(408450338,'Luis Retana','Heredia'),
                   (103680539,'Karen Pedregales','Heredia'),
                   (307670615,'Wilmer Gutiérrez','Cartago'),
                   (302840639,'Gilberto Andrade','Cartago'),
                   (109480531,'Leonora Avante','San José'),
                   (509060541,'Luis Cruz','Guanacaste')]
    ciclistas_2_df = spark_session.createDataFrame(ciclistas_2, schema = schema_cyclist)
    
    rutas_2 = [(9856712,'Grecia - Santa Bárbara',34.1),
               (5986387,'San Ramón - San Vicente',121.2),
               (8574581,'San Rafael - Naranjo',45.7),
               (2895769,'Liberia - San José',170.2),
               (3108542,'Heredia - Grecia',35.5)]
    rutas_2_df = spark_session.createDataFrame(rutas_2, schema = schema_route)

    actividad_2 = [(9856712, 408450338, '2026-12-02'),
                   (5986387, 103680539, '2026-09-07'),
                   (8574581, 109480531, '2026-08-14'),
                   (2895769, 509060541, '2026-03-19'),
                   (3108542, 302840639, '2026-09-06'),
                   (5986387, 307670615, '2026-10-21'),
                   (8574581, 307670615, '2026-08-23'),
                   (2895769, 109480531, '2026-07-07'),
                   (5986387, 302840639, '2026-06-01'),
                   (9856712, 509060541, '2026-11-11'),
                   (3108542, 307670615, '2026-06-15'),
                   (5986387, 109480531, '2026-05-24')]
    actividad_2_df = spark_session.createDataFrame(actividad_2, schema = schema_activity)

    correct_12_df = spark_session.createDataFrame([(408450338,'Luis Retana','Heredia', 9856712, '2026-12-02','Grecia - Santa Bárbara',34.1),
                                                   (103680539,'Karen Pedregales','Heredia', 5986387, '2026-09-07','San Ramón - San Vicente',121.2),
                                                   (109480531,'Leonora Avante','San José', 8574581, '2026-08-14','San Rafael - Naranjo',45.7),
                                                   (509060541,'Luis Cruz','Guanacaste', 2895769, '2026-03-19','Liberia - San José',170.2),
                                                   (302840639,'Gilberto Andrade','Cartago', 3108542, '2026-09-06','Heredia - Grecia',35.5),
                                                   (307670615,'Wilmer Gutiérrez','Cartago', 5986387, '2026-10-21','San Ramón - San Vicente',121.2),
                                                   (307670615,'Wilmer Gutiérrez','Cartago', 8574581, '2026-08-23','San Rafael - Naranjo',45.7),
                                                   (109480531,'Leonora Avante','San José', 2895769, '2026-07-07','Liberia - San José',170.2),
                                                   (302840639,'Gilberto Andrade','Cartago', 5986387, '2026-06-01','San Ramón - San Vicente',121.2),
                                                   (509060541,'Luis Cruz','Guanacaste', 9856712, '2026-11-11','Grecia - Santa Bárbara',34.1),
                                                   (307670615,'Wilmer Gutiérrez','Cartago', 3108542, '2026-06-15','Heredia - Grecia',35.5),
                                                   (109480531,'Leonora Avante','San José', 5986387, '2026-05-24','San Ramón - San Vicente',121.2)],
                                                   schema = schema_correct_1)

    correct_12_df = correct_12_df.withColumn("fecha", to_date(col("fecha")))

    output_t12_df = join_cyclist_activity_route(ciclistas_2_df, actividad_2_df, rutas_2_df)

    assertDataFrameEqual(output_t12_df, correct_12_df)


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# SCHEMAS TEST 2

schema_input_2 = StructType([
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("cod_ruta", IntegerType(), True),
            StructField("fecha", StringType(), True),
            StructField("nombre_ruta", StringType(), True),
            StructField("km", FloatType(), True)
])

schema_correct_2 = StructType([
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("fecha", StringType(), True),
            StructField("km", DoubleType(), True)
])


#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# TEST 2: Prueba de la función del join de dataframes

def test_partial_aggregate_1(spark_session):
    
    print("PRUEBA UNITARIA 2.1: Prueba 1 de la función de agregaciones parciales")

    input_t21_df = spark_session.createDataFrame([(207580425, 'Alonso Arguedas', 'Heredia', 484, '2024-02-18', 'Belén - Poás', 45.3),
                                                  (209190484, 'Elias Herrera', 'Alajuela', 703, '2024-03-21', 'San José de la Montaña - Sabanilla', 35.8),
                                                  (408470591, 'Marco Jimenez', 'Heredia', 795, '2024-04-22', 'Escazú - Cartago', 28.3),
                                                  (301780416, 'Javier Hernandez', 'Cartago', 932, '2024-05-08', 'Hatillo - Sabana', 10.6),
                                                  (118740876, 'Gabriel Vargas', 'San José', 165, '2024-06-09', 'Paraíso - Alajuela', 60.4),
                                                  (305840417, 'Fabián Solera', 'Cartago', 795, '2024-06-12', 'Escazú - Cartago', 28.3),
                                                  (601980756, 'Josue Granados', 'Puntarenas', 484, '2024-07-01', 'Belén - Poás', 45.3),
                                                  (705280357, 'Andrés Johnson', 'Limón', 165, '2024-08-09', 'Paraíso - Alajuela', 60.4),
                                                  (601980756, 'Josue Granados', 'Puntarenas', 703, '2024-10-23', 'San José de la Montaña - Sabanilla', 35.8),
                                                  (301780416, 'Javier Hernandez', 'Cartago', 484, '2024-10-05', 'Belén - Poás', 45.3),
                                                  (408470591, 'Marco Jimenez', 'Heredia', 795, '2024-11-17', 'Escazú - Cartago', 28.3),
                                                  (209190484, 'Elias Herrera', 'Alajuela', 484, '2024-06-13', 'Belén - Poás', 45.3),
                                                  (209190484, 'Elias Herrera', 'Alajuela', 484, '2024-06-13', 'Belén - Poás', 45.3),
                                                  (207580425, 'Alonso Arguedas', 'Heredia', 932, '2024-08-26', 'Hatillo - Sabana', 10.6),
                                                  (209190484, 'Elias Herrera', 'Alajuela', 795, '2024-09-29', 'Escazú - Cartago', 28.3),
                                                  (305840417, 'Fabián Solera', 'Cartago', 703, '2024-12-14', 'San José de la Montaña - Sabanilla', 35.8),
                                                  (116570249, 'Rebeca Chaves', 'Heredia', None, None, None, None)],
                                                  schema = schema_input_2)

    input_t21_df = input_t21_df.withColumn("fecha", to_date(col("fecha")))

    correct_21_df = spark_session.createDataFrame([(207580425,'Alonso Arguedas','Heredia','2024-02-18',45.3),
                                                   (209190484,'Elias Herrera','Alajuela','2024-03-21',35.8),
                                                   (408470591,'Marco Jimenez','Heredia','2024-04-22',28.3),
                                                   (301780416,'Javier Hernandez','Cartago','2024-05-08',10.6),
                                                   (118740876,'Gabriel Vargas','San José','2024-06-09',60.4),
                                                   (305840417,'Fabián Solera','Cartago','2024-06-12',28.3),
                                                   (601980756,'Josue Granados','Puntarenas','2024-07-01',45.3),
                                                   (705280357,'Andrés Johnson','Limón','2024-08-09',60.4),
                                                   (601980756,'Josue Granados','Puntarenas','2024-10-23',35.8),
                                                   (301780416,'Javier Hernandez','Cartago','2024-10-05',45.3),
                                                   (408470591,'Marco Jimenez','Heredia','2024-11-17',28.3),
                                                   (209190484,'Elias Herrera','Alajuela','2024-06-13',90.6),
                                                   (207580425,'Alonso Arguedas','Heredia','2024-08-26',10.6),
                                                   (209190484,'Elias Herrera','Alajuela','2024-09-29',28.3),
                                                   (305840417,'Fabián Solera','Cartago','2024-12-14',35.8),
                                                   (116570249,'Rebeca Chaves','Heredia',None,None)],
                                                   schema = schema_correct_2)
    
    correct_21_df = correct_21_df.withColumn("fecha", to_date(col("fecha")))

    output_t21_df = aggregate_kms_per_cyclist_and_date(input_t21_df)

    assertDataFrameEqual(output_t21_df, correct_21_df)
    #assert output_t21_df.collect() == correct_21_df.collect()


def test_partial_aggregate_2(spark_session):
    
    print("PRUEBA UNITARIA 2.2: Prueba 2 de la función de agregaciones parciales")

    input_t22_df = spark_session.createDataFrame([(408450338,'Luis Retana','Heredia', 9856712, '2026-12-02','Grecia - Santa Bárbara',34.1),
                                                  (103680539,'Karen Pedregales','Heredia', 5986387, '2026-09-07','San Ramón - San Vicente',121.2),
                                                  (109480531,'Leonora Avante','San José', 8574581, '2026-08-14','San Rafael - Naranjo',45.7),
                                                  (509060541,'Luis Cruz','Guanacaste', 2895769, '2026-03-19','Liberia - San José',170.2),
                                                  (302840639,'Gilberto Andrade','Cartago', 3108542, '2026-09-06','Heredia - Grecia',35.5),
                                                  (307670615,'Wilmer Gutiérrez','Cartago', 5986387, '2026-10-21','San Ramón - San Vicente',121.2),
                                                  (307670615,'Wilmer Gutiérrez','Cartago', 8574581, '2026-08-23','San Rafael - Naranjo',45.7),
                                                  (109480531,'Leonora Avante','San José', 2895769, '2026-07-07','Liberia - San José',170.2),
                                                  (302840639,'Gilberto Andrade','Cartago', 5986387, '2026-06-01','San Ramón - San Vicente',121.2),
                                                  (509060541,'Luis Cruz','Guanacaste', 9856712, '2026-11-11','Grecia - Santa Bárbara',34.1),
                                                  (307670615,'Wilmer Gutiérrez','Cartago', 3108542, '2026-06-15','Heredia - Grecia',35.5),
                                                  (109480531,'Leonora Avante','San José', 5986387, '2026-05-24','San Ramón - San Vicente',121.2)],
                                                  schema = schema_input_2)

    input_t22_df = input_t22_df.withColumn("fecha", to_date(col("fecha")))

    correct_22_df = spark_session.createDataFrame([(408450338,'Luis Retana','Heredia','2026-12-02',34.1),
                                                   (103680539,'Karen Pedregales','Heredia','2026-09-07',121.2),
                                                   (109480531,'Leonora Avante','San José','2026-08-14',45.7),
                                                   (509060541,'Luis Cruz','Guanacaste','2026-03-19',170.2),
                                                   (302840639,'Gilberto Andrade','Cartago','2026-09-06',35.5),
                                                   (307670615,'Wilmer Gutiérrez','Cartago','2026-10-21',121.2),
                                                   (307670615,'Wilmer Gutiérrez','Cartago','2026-08-23',45.7),
                                                   (109480531,'Leonora Avante','San José','2026-07-07',170.2),
                                                   (302840639,'Gilberto Andrade','Cartago','2026-06-01',121.2),
                                                   (509060541,'Luis Cruz','Guanacaste','2026-11-11',34.1),
                                                   (307670615,'Wilmer Gutiérrez','Cartago','2026-06-15',35.5),
                                                   (109480531,'Leonora Avante','San José','2026-05-24',121.2)],
                                                   schema = schema_correct_2)
    
    correct_22_df = correct_22_df.withColumn("fecha", to_date(col("fecha")))

    output_t22_df = aggregate_kms_per_cyclist_and_date(input_t22_df)

    assertDataFrameEqual(output_t22_df, correct_22_df)

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# SCHEMAS TEST 3
    
schema_input_3 = StructType([
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("provincia", StringType(), True),
            StructField("fecha", StringType(), True),
            StructField("km", FloatType(), True)
])

schema_correct_3 = StructType([
            StructField("rank", IntegerType(), True),
            StructField("provincia", StringType(), True),
            StructField("cedula", IntegerType(), True),
            StructField("nombre", StringType(), True),
            StructField("km_total", DoubleType(), True),
            StructField("km_prom_diario", DoubleType(), True)
])

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# TEST 3: Prueba de la función del join de dataframes
    
def test_final_results_1(spark_session):
    
    print("PRUEBA UNITARIA 3.1: Prueba 1 de la función de resultados finales")

    input_t31_df = spark_session.createDataFrame([(207580425,'Alonso Arguedas','Heredia','2024-02-18',45.3),
                                                  (209190484,'Elias Herrera','Alajuela','2024-03-21',35.8),
                                                  (408470591,'Marco Jimenez','Heredia','2024-04-22',28.3),
                                                  (301780416,'Javier Hernandez','Cartago','2024-05-08',10.6),
                                                  (118740876,'Gabriel Vargas','San José','2024-06-09',60.4),
                                                  (305840417,'Fabián Solera','Cartago','2024-06-12',28.3),
                                                  (601980756,'Josue Granados','Puntarenas','2024-07-01',45.3),
                                                  (705280357,'Andrés Johnson','Limón','2024-08-09',60.4),
                                                  (601980756,'Josue Granados','Puntarenas','2024-10-23',35.8),
                                                  (301780416,'Javier Hernandez','Cartago','2024-10-05',45.3),
                                                  (408470591,'Marco Jimenez','Heredia','2024-11-17',28.3),
                                                  (209190484,'Elias Herrera','Alajuela','2024-06-13',90.6),
                                                  (207580425,'Alonso Arguedas','Heredia','2024-08-26',10.6),
                                                  (209190484,'Elias Herrera','Alajuela','2024-09-29',28.3),
                                                  (305840417,'Fabián Solera','Cartago','2024-12-14',35.8),
                                                  (116570249,'Rebeca Chaves','Heredia',None,None)],
                                                  schema = schema_input_3)
    
    input_t31_df = input_t31_df.withColumn("fecha", to_date(col("fecha")))

    correct_31_df = spark_session.createDataFrame([(1, 'Alajuela', 209190484,'Elias Herrera', 154.7, 51.57),
                                                   (1, 'Cartago', 305840417,'Fabián Solera', 64.1, 32.05),
                                                   (2, 'Cartago', 301780416,'Javier Hernandez', 55.9, 27.95),
                                                   (1, 'Heredia', 408470591,'Marco Jimenez', 56.6, 28.3),
                                                   (2, 'Heredia', 207580425,'Alonso Arguedas', 55.9, 27.95),
                                                   (1, 'Limón', 705280357,'Andrés Johnson', 60.4, 60.4),
                                                   (1, 'Puntarenas', 601980756,'Josue Granados', 81.1, 40.55),
                                                   (1, 'San José', 118740876,'Gabriel Vargas', 60.4, 60.4)],
                                                   schema = schema_correct_3)

    output_t31_df = top_n_cyclists(input_t31_df, 3)

    assertDataFrameEqual(output_t31_df, correct_31_df)


def test_final_results_2(spark_session):

    print("PRUEBA UNITARIA 3.2: Prueba 2 de la función de resultados finales")

    input_t32_df = spark_session.createDataFrame([(408450338,'Luis Retana','Heredia','2026-12-02',34.1),
                                                  (103680539,'Karen Pedregales','Heredia','2026-09-07',121.2),
                                                  (109480531,'Leonora Avante','San José','2026-08-14',45.7),
                                                  (509060541,'Luis Cruz','Guanacaste','2026-03-19',170.2),
                                                  (302840639,'Gilberto Andrade','Cartago','2026-09-06',35.5),
                                                  (307670615,'Wilmer Gutiérrez','Cartago','2026-10-21',121.2),
                                                  (307670615,'Wilmer Gutiérrez','Cartago','2026-08-23',45.7),
                                                  (109480531,'Leonora Avante','San José','2026-07-07',170.2),
                                                  (302840639,'Gilberto Andrade','Cartago','2026-06-01',121.2),
                                                  (509060541,'Luis Cruz','Guanacaste','2026-11-11',34.1),
                                                  (307670615,'Wilmer Gutiérrez','Cartago','2026-06-15',35.5),
                                                  (109480531,'Leonora Avante','San José','2026-05-24',121.2)],
                                                  schema = schema_input_3)
    
    input_t32_df = input_t32_df.withColumn("fecha", to_date(col("fecha")))

    correct_32_df = spark_session.createDataFrame([(1, 'Cartago', 307670615,'Wilmer Gutiérrez', 202.4, 67.47),
                                                   (2, 'Cartago', 302840639,'Gilberto Andrade', 156.7, 78.35),
                                                   (1, 'Guanacaste', 509060541,'Luis Cruz', 204.3, 102.15),
                                                   (1, 'Heredia', 103680539,'Karen Pedregales', 121.2, 121.2),
                                                   (2, 'Heredia', 408450338,'Luis Retana', 34.1, 34.1),
                                                   (1, 'San José', 109480531,'Leonora Avante', 337.1, 112.37)],
                                                   schema = schema_correct_3)

    output_t32_df = top_n_cyclists(input_t32_df, 2)

    assertDataFrameEqual(output_t32_df, correct_32_df)