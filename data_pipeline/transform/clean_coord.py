from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml
import os

from utils.spark_path import get_latest_year_month_path, get_current_year_month_path

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_clean_coord")
    .master("spark://spark-master:7077")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# ============================================================
# Config
# ============================================================
CONFIG_PATH = "/opt/spark/jobs/config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

ROOT = cfg["data_lake"]["root"]
LAYERS = cfg["data_lake"]["layers"]

# input (bronze)
coord_src_base = os.path.join(
    ROOT, LAYERS["bronze"]["domains"]["coord"]["paths"]["parquet"]
)
coord_src_path = get_latest_year_month_path(spark, coord_src_base)

# output (silver clean)
coord_clean_base = os.path.join(
    ROOT, LAYERS["silver"]["clean"]["domains"]["coord_clean"]["paths"]["parquet"]
)
coord_clean_path = get_current_year_month_path(coord_clean_base)

partition_cols = LAYERS["silver"]["clean"]["domains"]["coord_clean"].get(
    "partition", ["region"]
)

# ============================================================
# LOAD
# ============================================================
coord_df = spark.read.parquet(coord_src_path)

# ============================================================
# CLEANING
# ============================================================
coord_clean_df = (
    coord_df
    .withColumn(
        "도로명주소",
        F.concat_ws(
            " ",
            F.col("시도명"),
            F.col("시군구명"),
            F.col("도로명"),
            F.concat(
                F.col("건물본번").cast("string"),
                F.when(
                    (F.col("건물부번").isNotNull()) & (F.col("건물부번") != "0") & (F.col("건물부번") != 0),
                    F.concat(F.lit("-"), F.col("건물부번").cast("string"))
                ).otherwise(F.lit(""))
            )
        )
    )
    .select(
        F.col("도로명주소"),
        F.expr("try_cast(nullif(trim(`X좌표`), '') as double)").alias("x_utmk"),
        F.expr("try_cast(nullif(trim(`Y좌표`), '') as double)").alias("y_utmk"),
        F.col("region"),
    )
    .dropDuplicates(["도로명주소"])
)

# ============================================================
# SAVE
# ============================================================
(
    coord_clean_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(coord_clean_path)
)

spark.stop()
