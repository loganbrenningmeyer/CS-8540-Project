import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, count, sum as spark_sum
from pyspark.sql.types import (
    ArrayType,
    StringType,
)

from utils.config import parse_args, load_config
from utils.paths import join_path

# -------------------------
# Parse config
# -------------------------
args = parse_args()
config = load_config(args.config)

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

def tokenize_text(text):
    if text is None:
        return []

    return [m.group(0).lower() for m in TOKEN_RE.finditer(text)]

tokenize_udf = udf(tokenize_text, ArrayType(StringType()))


spark = (
    SparkSession.builder
    .appName("BuildCleanTokenCountsFabric")
    .getOrCreate()
)

# -------------------------
# Load clean Wikipedia Parquet table
# -------------------------
clean_corpus_dir = config["paths"]["clean_corpus"]
out_dir = config["paths"]["out_dir"]

clean_docs_parquet_dir = join_path(clean_corpus_dir, "wikipedia_parquet")
clean_stats_parquet_dir = join_path(out_dir, "clean_stats_parquet")

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

clean_stats.write.mode("overwrite").parquet(clean_stats_parquet_dir)

spark.stop()
