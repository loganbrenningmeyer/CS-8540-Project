import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit, log

from utils.config import load_config
from utils.paths import join_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    args = parser.parse_args()

    config = load_config(args.config)

    spark = (
        SparkSession.builder
        .appName("BuildCleanRatioScoresFabric")
        .getOrCreate()
    )

    # -------------------------
    # Load reddit / clean stats Parquet tables
    # -------------------------
    out_dir = config["paths"]["out_dir"]

    clean_stats_parquet_dir = join_path(out_dir, "clean_stats_parquet")

    if args.mode == "global":
        reddit_stats_all_time_parquet_dir = join_path(out_dir, "reddit_stats_all_time_global_parquet")
        clean_ratio_scores_parquet_dir = join_path(out_dir, "clean_ratio_scores_global_parquet")
    else:
        reddit_stats_all_time_parquet_dir = join_path(out_dir, "reddit_stats_all_time_subreddit_parquet")
        clean_ratio_scores_parquet_dir = join_path(out_dir, "clean_ratio_scores_subreddit_parquet")

    reddit_stats_all_time = spark.read.parquet(reddit_stats_all_time_parquet_dir)
    clean_stats = spark.read.parquet(clean_stats_parquet_dir)

    # -------------------------
    # Compute clean ratio score
    # -- Set p_clean for tokens absent from Wikipedia to a very low value for
    #    stability.
    #
    # => clean_ratio_score = log(p_reddit / p_clean)
    # -------------------------
    eps = 1e-12

    candidates = (
        reddit_stats_all_time
        .join(clean_stats, on="token", how="left")
        .withColumn("p_clean", coalesce(col("p_clean"), lit(eps)))
        .withColumn("clean_ratio_score", log(col("p_reddit") / col("p_clean")))
    )

    # -------------------------
    # Save clean_ratio_scores to Parquet
    # -------------------------
    candidates.write.mode("overwrite").parquet(clean_ratio_scores_parquet_dir)

    candidates.orderBy(col("clean_ratio_score").desc()).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
