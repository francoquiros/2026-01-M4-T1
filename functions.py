from pyspark.sql.functions import col, dense_rank, sum, mean, col
from pyspark.sql.window import Window

def join_dfs(df_1, df_2, df_3, on_column1, on_column2, type1 = 'inner', type2 = 'inner'):

    # Función que une tres dataframes según las columnas y el tipo de join indicados por el usuario.
    # Retorna un dataframe con los datos unidos de los tres dataframes.

    return joined_dataframe


def aggregate():
    # Función que agrega filas a un dataframe

    return y


def top_n(var_organizing, top_n_rows):
    # Función que ordena el dataframe manejado de acuerdo a una variable específica
    # y extrae las primeras "n" filas 

    return top_n_dataframe