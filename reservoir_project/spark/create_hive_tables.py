from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    lag,
    when,
    date_format,
    round as spark_round
)
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("CreateReservoirHiveTables")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://localhost:9000/user/talentum/reservoir_project/cleaned"


print("Reading cleaned Parquet...")

df = spark.read.parquet(INPUT)

print("Rows before de-duplication:", df.count())


# ---------------------------------------------
# De-duplication on (Reservoir_name, Date)
# ---------------------------------------------
# Multiple rows can land on the same (Reservoir_name, Date) if the
# producer/streaming job is re-run, or the same reservoir/date is
# reported in more than one source file. Keep exactly one row per
# key: prefer the row that actually has a Storage value, and break
# any remaining tie deterministically by source_file so re-running
# this job always produces the same result.
dedup_window = Window.partitionBy("Reservoir_name", "Date").orderBy(
    col("Storage").isNull().asc(),   # rows with a real Storage value first
    col("source_file").desc()
)

df = (
    df
    .withColumn("_rn", row_number().over(dedup_window))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

print("Rows after de-duplication:", df.count())

df.printSchema()


# ---------------------------------------------
# Derived columns
# ---------------------------------------------

# Month_Name, e.g. 1 -> "January" (nice for Power BI slicers/axes)
df = df.withColumn("Month_Name", date_format(col("Date"), "MMMM"))

# Storage_Percentage = Storage / Live_capacity_FRL * 100
# Left NULL (never 0) when Storage or capacity is missing, or capacity
# is 0 -- so Power BI can tell "no data" apart from "empty reservoir".
df = df.withColumn(
    "Storage_Percentage",
    when(
        col("Storage").isNotNull()
        & col("Live_capacity_FRL").isNotNull()
        & (col("Live_capacity_FRL") != 0),
        spark_round((col("Storage") / col("Live_capacity_FRL")) * 100, 2)
    ).otherwise(None)
)

# Previous_Storage / Storage_Change / Storage_Change_Percentage:
# period-over-period change per reservoir, ordered by Date. NULL
# (not 0) for a reservoir's first observation, since there is no
# previous value to compare against.
trend_window = Window.partitionBy("Reservoir_name").orderBy("Date")

df = df.withColumn("Previous_Storage", lag("Storage").over(trend_window))

df = df.withColumn(
    "Storage_Change",
    when(
        col("Storage").isNotNull() & col("Previous_Storage").isNotNull(),
        spark_round(col("Storage") - col("Previous_Storage"), 2)
    ).otherwise(None)
)

df = df.withColumn(
    "Storage_Change_Percentage",
    when(
        col("Storage").isNotNull()
        & col("Previous_Storage").isNotNull()
        & (col("Previous_Storage") != 0),
        spark_round((col("Storage_Change") / col("Previous_Storage")) * 100, 2)
    ).otherwise(None)
)


# ---------------------------------------------
# Database
# ---------------------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS reservoir_db")


# ---------------------------------------------
# Fact table
# ---------------------------------------------

fact_df = df.select(
    "Reservoir_name",
    "Basin",
    "Agency_name",
    "Lat",
    "Long",
    "Date",
    "Year",
    "Month",
    "Month_Name",
    "Full_reservoir_level",
    "Live_capacity_FRL",
    "Storage",
    "Level",
    "Storage_Percentage",
    "Previous_Storage",
    "Storage_Change",
    "Storage_Change_Percentage",
    "source_file"
)

fact_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("reservoir_db.fact_reservoir_level")


# ---------------------------------------------
# Dimension table
# ---------------------------------------------

dim_window = Window.partitionBy("Reservoir_name").orderBy("Date")

dim_df = (
    df
    .withColumn("rn", row_number().over(dim_window))
    .filter("rn = 1")
    .select(
        "Reservoir_name",
        "Basin",
        "Agency_name",
        "Lat",
        "Long"
    )
)

dim_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("reservoir_db.dim_reservoir")


print("Hive tables created.")

spark.sql("SHOW TABLES IN reservoir_db").show()

# Analytical VIEWs (yearly/monthly summary, latest status, capacity
# utilization, change, basin summary) + the data-quality query are
# created separately by sql/reservoir_tables.sql, run via
# scripts/09_create_hive_views.sh -- they read from the fact/dim
# tables built above, so this script must run first.

spark.stop()
