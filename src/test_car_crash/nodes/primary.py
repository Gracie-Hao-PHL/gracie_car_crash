import pyspark.sql.functions as F
from pyspark.sql import Window, SparkSession

from test_car_crash.nodes.utils import convert_datetime


def unix_to_datetime(data):
    """``unix_to_datetime`` converts unix timestamp to datetime, drop index

    Args:
        data (spark csv): raw csv file

    Returns:
        processed spark dataframe

    """
    return convert_datetime(data).drop("index", "time")


def prm_spine(df_car_location):
    """``prm_spine`` generate the spine dataframe

    Args:
        df_car_location (spark dataframe): primary car location dataframe

    Returns:
        df_mod_start (spark dataframe): spine dataframe

    """
    df_car_location.createOrReplaceTempView("car_location")

    spark = SparkSession.builder.getOrCreate()
    df_mod_start = spark.sql(
        """
            SELECT driver_id,
                   source,
                   min(dt_time) as mod_start_date,
                   max(dt_time) as pred_end_date
            FROM car_location
            WHERE source == 'San_Francisco'
            GROUP BY driver_id, source
            HAVING DATEDIFF(pred_end_date, mod_start_date) > 15

            UNION ALL

            SELECT driver_id,
                   source,
                   min(dt_time) as mod_start_date,
                   max(dt_time) as pred_end_date
            FROM car_location
            WHERE source == 'Rome'
            GROUP BY driver_id, source
            HAVING DATEDIFF(pred_end_date, mod_start_date) > 7.5
        """
    )
    df_final = (
        df_mod_start.withColumn(
            "mod_end_date",
            F.when(
                df_mod_start.source == "Rome",
                F.col("mod_start_date") + F.expr("INTERVAL 120 HOURS"),
            ).otherwise(F.col("mod_start_date") + F.expr("INTERVAL 240 HOURS")),
        )
        .withColumn(
            "pred_start_date",
            F.when(
                df_mod_start.source == "Rome",
                F.col("mod_end_date") + F.expr("INTERVAL 12 HOURS"),
            ).otherwise(F.col("mod_end_date") + F.expr("INTERVAL 24 HOURS")),
        )
        .withColumn(
            "pred_end_date",
            F.when(
                df_mod_start.source == "Rome",
                F.col("pred_start_date") + F.expr("INTERVAL 60 HOURS"),
            ).otherwise(F.col("pred_start_date") + F.expr("INTERVAL 120 HOURS")),
        )
        .drop("source")
    )

    return df_final
