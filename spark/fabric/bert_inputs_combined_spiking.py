import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

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
        .appName("BuildBertInputsCombinedSpikingFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        candidate_name = "slang_candidates_clean_ratio_spiking_combined_global_parquet"
        bert_output_name = "bert_inputs_clean_ratio_spiking_combined_global_parquet"
    else:
        candidate_name = "slang_candidates_clean_ratio_spiking_combined_subreddit_parquet"
        bert_output_name = "bert_inputs_clean_ratio_spiking_combined_subreddit_parquet"

    candidates_dir = join_path(out_dir, candidate_name)
    bert_output_dir = join_path(out_dir, bert_output_name)
    reddit_tokens_dir = join_path(out_dir, "reddit_tokens_subset_10pct_parquet")
    clean_posts_dir = join_path(out_dir, "reddit_clean_posts_parquet")

    reddit_tokens = spark.read.parquet(reddit_tokens_dir)
    candidates = spark.read.parquet(candidates_dir)
    clean_posts = spark.read.parquet(clean_posts_dir)

    if args.mode == "subreddit":
        join_cols = ["subreddit", "token"]
    else:
        join_cols = ["token"]

    bert_inputs = (
        reddit_tokens
        .join(candidates.select(*join_cols).distinct(), on=join_cols, how="inner")
        .join(
            clean_posts.select("post_id", col("body").alias("full_text")),
            on="post_id",
            how="left",
        )
        .select(
            "post_id",
            "subreddit",
            "timestamp",
            "token",
            "token_start",
            "token_end",
            "full_text",
            lit(True).alias("is_slang_candidate"),
        )
    )

    bert_inputs.write.mode("overwrite").parquet(bert_output_dir)

    print("wrote_bert_inputs={}".format(bert_output_dir))

    spark.stop()


if __name__ == "__main__":
    main()
