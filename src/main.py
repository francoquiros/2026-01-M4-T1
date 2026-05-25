# Imports

from pyspark.sql import SparkSession

import sys
from functions import load_cyclists, load_routes, load_activities


def validate_args():
    errors = []
    if len(sys.argv) != 4:
        errors.append("Error: Se requieren exactamente 3 argumentos")
    if errors:
        for error in errors:
            print(error)
        return False
    return True


def main():
    if not validate_args():
        sys.exit(1)

    # Inputs para crear los dataframes
    ciclistas_path = sys.argv[1]
    rutas_path = sys.argv[2]
    actividad_path = sys.argv[3]

    # -----------------------------------------------------
    # Create Spark session
    # -----------------------------------------------------

    spark = SparkSession.builder.appName("Tarea1").master("local[*]").getOrCreate()
    # -----------------------------------------------------
    # Load input data
    # -----------------------------------------------------

    ciclistas_df = load_cyclists(spark, ciclistas_path)
    rutas_df = load_routes(spark, rutas_path)
    actividad_df = load_activities(spark, actividad_path)

    print("ciclistas")
    ciclistas_df.show(10)
    print("rutas")
    rutas_df.show(10)
    print("actividad")
    actividad_df.show(10)

    # -----------------------------------------------------
    # Data joins
    # -----------------------------------------------------
    # TODO:
    #
    # joined_df = ...
    # -----------------------------------------------------
    # Intermediate aggregations
    # -----------------------------------------------------
    # TODO:
    #
    # totals_df = ...
    # -----------------------------------------------------
    # Final calculations
    # -----------------------------------------------------
    # TODO:
    #
    # top_cyclists_df = ...
    # -----------------------------------------------------
    # Show results
    # -----------------------------------------------------
    # TODO:
    #
    # top_cyclists_df.show()
    # -----------------------------------------------------
    # Export results (optional)
    # -----------------------------------------------------
    # TODO:
    #
    # top_cyclists_df.write...
    # -----------------------------------------------------
    # Stop Spark session
    # -----------------------------------------------------
    spark.stop()


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":
    main()
