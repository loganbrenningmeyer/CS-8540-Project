import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit, when

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
        .appName("CombineSpikingCandidatesFabric")
        .getOrCreate()
    )

    out_dir = config["paths"]["out_dir"]

    if args.mode == "global":
        candidate_key_cols = ["corpus", "token"]
        monthly_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_monthly_global_parquet",
        )
        yearly_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_yearly_global_parquet",
        )
        combined_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_combined_global_parquet",
        )
    else:
        candidate_key_cols = ["subreddit", "token"]
        monthly_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_monthly_subreddit_parquet",
        )
        yearly_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_yearly_subreddit_parquet",
        )
        combined_dir = join_path(
            out_dir,
            "slang_candidates_clean_ratio_spiking_combined_subreddit_parquet",
        )

    monthly = spark.read.parquet(monthly_dir)
    yearly = spark.read.parquet(yearly_dir)

    base_keys = (
        monthly
        .select(*candidate_key_cols)
        .union(yearly.select(*candidate_key_cols))
        .distinct()
    )

    monthly_select = (
        monthly
        .select(
            *candidate_key_cols,
            col("count_reddit_all_time").alias("count_reddit_all_time_monthly"),
            col("total_tokens_all_time").alias("total_tokens_all_time_monthly"),
            col("p_reddit_all_time").alias("p_reddit_all_time_monthly"),
            col("count_clean").alias("count_clean_monthly"),
            col("total_clean_tokens").alias("total_clean_tokens_monthly"),
            col("p_clean").alias("p_clean_monthly"),
            col("clean_ratio_score").alias("clean_ratio_score_monthly"),
            col("max_spike_score").alias("max_spike_score_monthly"),
            col("num_spike_periods").alias("num_spike_periods_monthly"),
        )
    )

    yearly_select = (
        yearly
        .select(
            *candidate_key_cols,
            col("count_reddit_all_time").alias("count_reddit_all_time_yearly"),
            col("total_tokens_all_time").alias("total_tokens_all_time_yearly"),
            col("p_reddit_all_time").alias("p_reddit_all_time_yearly"),
            col("count_clean").alias("count_clean_yearly"),
            col("total_clean_tokens").alias("total_clean_tokens_yearly"),
            col("p_clean").alias("p_clean_yearly"),
            col("clean_ratio_score").alias("clean_ratio_score_yearly"),
            col("max_spike_score").alias("max_spike_score_yearly"),
            col("num_spike_periods").alias("num_spike_periods_yearly"),
        )
    )

    combined = (
        base_keys
        .join(monthly_select, on=candidate_key_cols, how="left")
        .join(yearly_select, on=candidate_key_cols, how="left")
        .withColumn(
            "count_reddit_all_time",
            coalesce(
                col("count_reddit_all_time_monthly"),
                col("count_reddit_all_time_yearly"),
            ),
        )
        .withColumn(
            "total_tokens_all_time",
            coalesce(
                col("total_tokens_all_time_monthly"),
                col("total_tokens_all_time_yearly"),
            ),
        )
        .withColumn(
            "p_reddit_all_time",
            coalesce(
                col("p_reddit_all_time_monthly"),
                col("p_reddit_all_time_yearly"),
            ),
        )
        .withColumn(
            "count_clean",
            coalesce(col("count_clean_monthly"), col("count_clean_yearly")),
        )
        .withColumn(
            "total_clean_tokens",
            coalesce(
                col("total_clean_tokens_monthly"),
                col("total_clean_tokens_yearly"),
            ),
        )
        .withColumn(
            "p_clean",
            coalesce(col("p_clean_monthly"), col("p_clean_yearly")),
        )
        .withColumn(
            "clean_ratio_score",
            coalesce(
                col("clean_ratio_score_monthly"),
                col("clean_ratio_score_yearly"),
            ),
        )
        .withColumn(
            "has_monthly_spike",
            col("max_spike_score_monthly").isNotNull(),
        )
        .withColumn(
            "has_yearly_spike",
            col("max_spike_score_yearly").isNotNull(),
        )
        .withColumn(
            "spike_profile",
            when(
                col("has_monthly_spike") & col("has_yearly_spike"),
                lit("both"),
            )
            .when(col("has_monthly_spike"), lit("monthly_only"))
            .when(col("has_yearly_spike"), lit("yearly_only"))
            .otherwise(lit("none")),
        )
        .withColumn(
            "is_slang_candidate_clean_ratio_spiking_combined",
            lit(True),
        )
        .select(
            *candidate_key_cols,
            "count_reddit_all_time",
            "total_tokens_all_time",
            "p_reddit_all_time",
            "count_clean",
            "total_clean_tokens",
            "p_clean",
            "clean_ratio_score",
            "has_monthly_spike",
            "max_spike_score_monthly",
            "num_spike_periods_monthly",
            "has_yearly_spike",
            "max_spike_score_yearly",
            "num_spike_periods_yearly",
            "spike_profile",
            "is_slang_candidate_clean_ratio_spiking_combined",
        )
    )

    combined.write.mode("overwrite").parquet(combined_dir)

    print("wrote_combined={}".format(combined_dir))

    spark.stop()


if __name__ == "__main__":
    main()
