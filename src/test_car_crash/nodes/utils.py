import pyspark.sql.functions as F
import pyspark.sql.types as T
from haversine import haversine


def read_input(data):
    return data


def convert_datetime(df):
    df = df.withColumn("dt_time", F.to_timestamp(df.time / 1000))
    return df


def clean_column_names(data):
    df = data

    for col in data.columns:
        df = df.withColumnRenamed(col, col.replace(":", "_"))

    return df


def distance(coord1, coord2):
    try:
        return haversine(coord1, coord2, unit="mi")
    except:
        return None


def find_time_zone(source):
    if source == "ROME":
        timezone = "Europe/Rome"
    else:
        timezone = "America/Los_Angeles"
    return timezone


list_of_tags = ["highway", "name"]


def extract_tags(tags):
    column_dict = dict.fromkeys(list_of_tags, "")
    i = 0
    while i < len(list_of_tags):
        for tag in tags:
            if tag._k == list_of_tags[i]:
                column_dict[list_of_tags[i]] = tag._v
        i += 1
    return [v for v in column_dict.values()]


schema = T.StructType(
    [T.StructField(tag, T.StringType(), False) for tag in list_of_tags]
)

udf_extract_tags = F.udf(extract_tags, schema)
