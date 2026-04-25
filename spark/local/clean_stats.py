import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, sum as spark_sum
from pyspark.sql.types import (
    ArrayType,
    StringType,
)

# -------------------------
# Word-level Regex:
# - \b --> Start word boundary
# - [\w]+ --> One or more word characters
# - (?:[-'][\w]+)* --> Optionally capture repeated group: hyphenated or apostrophes (e.g., rock-and-roll, y'all'd've)
# - \b --> End word boundary
#
# Allows capturing words like:
# - lmaooo!! -> lmaooo
# - foo_bar -> foo_bar
# - don't -> don't
# - y'all -> y'all 
# - hi!hello -> hi, hello
# - rock-and-roll -> rock-and-roll
# - low-key -> low-key
# -------------------------
TOKEN_RE = re.compile(r"\b[\w]+(?:[-'][\w]+)*\b", re.UNICODE)

def tokenize_text(text: str) -> list[str]:
    if text is None:
        return []

    return [m.group(0).lower() for m in TOKEN_RE.finditer(text)]

tokenize_udf = udf(tokenize_text, ArrayType(StringType()))


spark = (
    SparkSession.builder
    .appName("BuildCleanTokenCountsLocal")
    .master("local[4]")
    .getOrCreate()
)

# -------------------------
# Load clean Wikipedia Parquet table
# -------------------------
data_dir = Path("/home/logan/projects/CS-8540-Project/spark/data")
out_dir = Path("/home/logan/projects/CS-8540-Project/spark/outputs")

clean_docs_parquet_dir = str(data_dir / "wikipedia_parquet")
clean_stats_parquet_dir = str(out_dir / "clean_stats_parquet")
clean_stats_csv_dir = str(out_dir / "clean_stats_csv")

docs = spark.read.parquet(clean_docs_parquet_dir)

# -------------------------
# Create clean tokens Spark DataFrame / count tokens
# -------------------------
tokens = (
    docs
    .select(explode(tokenize_udf(col("text"))).alias("token"))
    .filter(col("token") != "")
)

token_counts = (
    tokens
    .groupBy("token")
    .agg(count("*").alias("count_clean"))
)

# -------------------------
# Count overall total clean tokens
# -------------------------
total_tokens = token_counts.agg(
    spark_sum("count_clean").alias("total_clean_tokens")
).collect()[0]["total_clean_tokens"]

# -------------------------
# Compute p_clean for each clean token
# -------------------------
clean_stats = (
    token_counts
    .withColumn("total_clean_tokens", col("count_clean") * 0 + total_tokens)
    .withColumn("p_clean", col("count_clean") / col("total_clean_tokens"))
)

clean_stats.orderBy(col("count_clean").desc()).show(50, truncate=False)

# -------------------------
# Write clean_stats to csv / Parquet
# -------------------------
(
    clean_stats
    .orderBy(col("count_clean").desc())
    .limit(100)
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(clean_stats_csv_dir)
)

clean_stats.write.mode("overwrite").parquet(clean_stats_parquet_dir)

spark.stop()