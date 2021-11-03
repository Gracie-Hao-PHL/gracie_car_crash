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
        .select("driver_id", "mod_start_date","crash","source", "dt_time")

    return df_fea_prev_crash
