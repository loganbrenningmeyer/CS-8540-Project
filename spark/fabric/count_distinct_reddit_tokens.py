from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

from utils.config import parse_args, load_config
from utils.paths import join_path


args = parse_args()
config = load_config(args.config)

spark = (
    SparkSession.builder
    .appName("CountDistinctRedditTokensFabric")
    .getOrCreate()
)

out_dir = config["paths"]["out_dir"]
tokens_parquet_dir = join_path(out_dir, "reddit_tokens_parquet")

tokens = spark.read.parquet(tokens_parquet_dir)

distinct_token_count = (
    tokens
    .agg(countDistinct("token").alias("distinct_token_count"))
    .collect()[0]["distinct_token_count"]
)

print("distinct_token_count={}".format(distinct_token_count))

spark.stop()
