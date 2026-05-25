from pyspark.sql.functions import col, dense_rank, sum, mean, col
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    FloatType,
    DateType,
)


def load_cyclists(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load cyclists CSV file.
    """
    # Define schema
    schema = StructType(
        [
            StructField("cedula", IntegerType(), False),
            StructField("nombre_completo", StringType(), False),
            StructField("provincia", StringType(), False),
        ]
    )
    # Read CSV
    df = spark.read.csv(file_path, header=False, schema=schema)
    # Return dataframe
    return df


def load_routes(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load routes CSV file.
    """
    # Define schema
    schema = StructType(
        [
            StructField("codigo_ruta", IntegerType(), False),
            StructField("nombre_ruta", StringType(), False),
            StructField("kilometros", FloatType(), False),
        ]
    )
    # Read CSV
    df = spark.read.csv(file_path, header=False)
    # Return dataframe
    return df


def load_activities(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load activities CSV file.
    """
    # Define schema
    schema = StructType(
        [
            StructField("codigo_ruta", IntegerType(), False),
            StructField("cedula", IntegerType(), False),
            StructField("fecha", DateType(), False),
        ]
    )
    # Read CSV
    df = spark.read.csv(file_path, header=False, schema=schema)
    # Return dataframe
    return df


# =========================================================
# Join functions
# =========================================================
def join_data(
    cyclists_df: DataFrame, routes_df: DataFrame, activities_df: DataFrame
) -> DataFrame:
    """
    Join all input datasets.
    """
    # TODO:
    #
    # Join cyclists with activities
    # Join result with routes
    # Return final dataframe
    pass


# =========================================================
# Aggregation functions
# =========================================================
def calculate_total_kilometers(joined_df: DataFrame) -> DataFrame:
    """
    Calculate total kilometers by cyclist.
    """
    # TODO:
    #
    # Group data
    # Sum kilometers
    # Return dataframe
    pass


def calculate_daily_average(joined_df: DataFrame) -> DataFrame:
    """
    Calculate average daily kilometers.
    """
    # TODO:
    #
    # Aggregate by date
    # Calculate averages
    # Return dataframe
    pass


def calculate_province_totals(joined_df: DataFrame) -> DataFrame:
    """
    Calculate total kilometers by province.
    """
    # TODO:
    #
    # Group by province
    # Sum kilometers
    # Return dataframe
    pass


# =========================================================
# Ranking functions
# =========================================================
def get_top_cyclists_by_total_km(totals_df: DataFrame, top_n: int = 5) -> DataFrame:
    """
    Return top cyclists by total kilometers.
    """
    # TODO:
    #
    # Order dataframe
    # Limit top N
    # Return dataframe
    pass


def get_top_cyclists_by_daily_average(
    averages_df: DataFrame, top_n: int = 5
) -> DataFrame:
    """
    Return top cyclists by daily average.
    """
    # TODO:
    #
    # Order dataframe
    # Limit top N
    # Return dataframe
    pass


def validate_dataframe(df: DataFrame) -> bool:
    """
    Validate dataframe content.
    """
    # TODO:
    #
    # Validate nulls
    # Validate schema
    # Validate duplicates
    pass
