import argparse

from pyspark.sql import SparkSession

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildRedditCleanPostsSubsetFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    reddit_tokens_subset_dir = join_path(out_dir, "reddit_tokens_subset_10pct_parquet")
    clean_posts_dir = join_path(out_dir, "reddit_clean_posts_parquet")
    clean_posts_subset_dir = join_path(out_dir, "reddit_clean_posts_subset_10pct_parquet")

    reddit_tokens_subset = spark.read.parquet(reddit_tokens_subset_dir)
    clean_posts = spark.read.parquet(clean_posts_dir)

    subset_post_ids = reddit_tokens_subset.select("post_id").distinct()

    clean_posts_subset = clean_posts.join(
        subset_post_ids,
        on="post_id",
        how="inner",
    )

    clean_posts_subset.write.mode("overwrite").parquet(clean_posts_subset_dir)

    print("wrote_subset={}".format(clean_posts_subset_dir))

    spark.stop()


if __name__ == "__main__":
    main()
