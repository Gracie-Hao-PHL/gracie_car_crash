# QUANTUMBLACK CONFIDENTIAL
#
# Copyright (c) 2016 - present QuantumBlack Visual Analytics Ltd. All
# Rights Reserved.
#
# NOTICE: All information contained herein is, and remains the property of
# QuantumBlack Visual Analytics Ltd. and its suppliers, if any. The
# intellectual and technical concepts contained herein are proprietary to
# QuantumBlack Visual Analytics Ltd. and its suppliers and may be covered
# by UK and Foreign Patents, patents in process, and are protected by trade
# secret or copyright law. Dissemination of this information or
# reproduction of this material is strictly forbidden unless prior written
# permission is obtained from QuantumBlack Visual Analytics Ltd.

import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Window

from test_car_crash.nodes.utils import distance


def fea_total_miles(df_spine, df_location):
    """``fea_total_miles`` calculates the total miles driven during the modeling period

        Args:
            df_spine (spark dataframe): initial spine dataframe
            df_location (spark dataframe): primary location dataframe

        Returns:
            df_fea_total_miles (spark dataframe): total miles driver per driver_id within modeling period

    """

    df_location_filtered = df_spine.join(
        df_location,
        (df_spine.driver_id == df_location.driver_id)
        & (df_location.dt_time.between(df_spine.mod_start_date, df_spine.mod_end_date)),
    ).drop(df_spine.driver_id)

    df_location_unix_time = df_location_filtered.withColumn(
        "unix_time",
        F.unix_timestamp(
            df_location_filtered.dt_time, format="yyyy-MM-dd HH:mm:ss.SSS"
        ),
    )

    my_window = Window.partitionBy("driver_id").orderBy("unix_time")

    df_location_lag = (
        df_location_unix_time.withColumn(
            "prev_lat", F.lag(df_location_unix_time.latitude).over(my_window)
        )
        .withColumn("prev_long", F.lag(df_location_unix_time.longitude).over(my_window))
        .withColumn("car_coord", F.struct(F.col("latitude"), F.col("longitude")))
    )

    df_location_diff = (
        df_location_lag.withColumn(
            "lat_diff",
            (
                F.when(
                    F.isnull(df_location_lag.latitude - df_location_lag.prev_lat), None
                ).otherwise(df_location_lag.latitude - df_location_lag.prev_lat)
            ),
        )
        .withColumn(
            "long_diff",
            (
                F.when(
                    F.isnull(df_location_lag.longitude - df_location_lag.prev_long),
                    None,
                ).otherwise(df_location_lag.longitude - df_location_lag.prev_long)
            ),
        )
        .withColumn("prev_car_coord", F.struct(F.col("prev_lat"), F.col("prev_long")))
    )

    udf_distance = F.udf(distance, DoubleType())
    df_distance = df_location_diff.withColumn(
        "distance",
        udf_distance(df_location_diff.car_coord, df_location_diff.prev_car_coord),
    ).na.fill(0)

    df_total_miles = df_distance.groupBy("driver_id").agg(
        F.sum("distance").alias("total_miles")
    )

    df_fea_total_miles = (
        df_spine.join(
            df_total_miles, df_spine.driver_id == df_total_miles.driver_id, "inner"
        )
        .drop(df_total_miles.driver_id)
        .select("driver_id", "mod_start_date", "total_miles")
    )

    return df_fea_total_miles

def fea_rush_hour_ratio(df_spine, df_location):
    """``rush_hour_ratio`` calculates the ratio of rush hour driving during the modeling period

        Args:
            df_spine (spark dataframe): initial spine dataframe
            df_location (spark dataframe): primary location dataframe

        Returns:
            df_fea_rush_hour_ratio (spark dataframe): Ratio of rush hour driving vs non rush hour driving during the modeling period 

    """
    #df_location = df_location.withColumn("date_time", F.to_timestamp(df_location.unix_time / 1000))

    # Filter location data between mod_start_date and mod_end_date
    df_location_filtered = df_spine.join(
            df_location,
            ((df_spine.driver_id == df_location.driver_id)
            & (df_location['dt_time'].between(df_spine.mod_start_date, df_spine.mod_end_date))),
        'left').drop(df_spine.driver_id)
    # Calculate rush hour
    df_location_filtered = df_location_filtered.withColumn('time',F.date_format('dt_time','HH:mm:ss'))
    df_rush_hour = df_location_filtered.withColumn('RushHour',
                          F.when(((df_location_filtered['time'] >= '07:00:00')&(df_location_filtered['time'] <= '09:00:00'))|
                                 ((df_location_filtered['time'] >= '16:30:00')&(df_location_filtered['time'] <= '19:30:00')),
                                 1)
                                )
    df_rush_hour = df_rush_hour.fillna(0,'RushHour')
    # Calculate rush ratio
    df_rush_ratio = df_rush_hour.groupBy('driver_id').agg((F.sum(df_rush_hour['RushHour'])/F.count(df_rush_hour['RushHour'])).alias('rush_hour_ratio'))
    df_fea_rush_hour_ratio = (
            df_spine.join(
                df_rush_ratio, df_spine.driver_id == df_rush_ratio.driver_id, "inner"
            )
            .drop(df_rush_ratio.driver_id)
            .select("driver_id", "mod_start_date", "rush_hour_ratio")
    )
    return df_fea_rush_hour_ratio

def fea_prev_crash(df_spine, df_crash):
    """Calculate the number of crashes each driver had during the modeling period

    Args:
        df_spine (spark dataframe): [description]
        df_crash (spark dataframe): [description]

    Returns:
        df_fea_prev_crash(spark dataframe): [description]
    """
    df_fea_prev_crash = df_spine\
        .join(df_crash,\
            ((df_spine.driver_id == df_crash.driver_id)\
            & (df_crash['dt_time'].between(df_spine.mod_start_date, df_spine.mod_end_date))),\
            'left').drop(df_spine.driver_id)\
        .withColumnRenamed('dt_time', 'crash_ts')\
        .select("driver_id", "mod_start_date","crash", "source", "crash_ts")

    return df_fea_prev_crash

