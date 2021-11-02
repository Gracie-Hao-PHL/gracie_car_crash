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


def mod_master(
    df_spine,
    df_total_miles,
    df_fea_rush_hour_ratio
):

    key_cols = ("driver_id", "mod_start_date")

    df_joined = (
        df_spine
        .join(df_total_miles, [*key_cols], "left")
        .join(df_fea_rush_hour_ratio, [*key_cols], "left")
    )

    return df_joined
