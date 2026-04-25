import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    coalesce,
    lit,
    max as spark_max,
)

from utils.config import load_config
from utils.paths import join_path


# -------------------------
# Candidate thresholds
# -------------------------
# -- MIN_TOKEN_COUNT:
#       Minimum all-time Reddit support for a token.
#       This prevents very rare one-off tokens from being labeled as slang.
#
# -- MIN_CLEAN_RATIO_SCORE:
#       Minimum clean-ratio score required for the broad slang candidate table.
#       2.3 ~= log(10), so the token is about 10x more common in Reddit than
#       in the clean Wikipedia corpus.
#
# -- MIN_SPIKE_SCORE:
#       Minimum spike score required for the stricter "emergent" slang table.
#       1.6 ~= log(5), so the token reached about 5x its recent baseline.
#
# -- MIN_SPIKE_PERIODS:
#       Minimum number of monthly/yearly periods where the token passes
#       MIN_SPIKE_SCORE. Keep this at 1 if a single strong spike period is
#       enough evidence.
# -------------------------
MIN_TOKEN_COUNT = 100
MIN_CLEAN_RATIO_SCORE = 3.0
MIN_SPIKE_SCORE = 2.3
MIN_SPIKE_PERIODS = 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument("--min-token-count", type=int, default=None)
    parser.add_argument("--min-clean-ratio-score", type=float, default=None)
    parser.add_argument("--min-spike-score", type=float, default=None)
    parser.add_argument("--min-spike-periods", type=int, default=None)
    parser.add_argument("--spike-time-grain", choices=["monthly", "yearly"], default="monthly")
    args = parser.parse_args()

    config = load_config(args.config)
    params = config.get("params", {})

    min_token_count = args.min_token_count
    if min_token_count is None:
        min_token_count = params.get("min_token_count", MIN_TOKEN_COUNT)

    min_clean_ratio_score = args.min_clean_ratio_score
    if min_clean_ratio_score is None:
        min_clean_ratio_score = params.get("min_clean_ratio_score", MIN_CLEAN_RATIO_SCORE)

    min_spike_score = args.min_spike_score
    if min_spike_score is None:
        min_spike_score = params.get("min_spike_score", MIN_SPIKE_SCORE)

    min_spike_periods = args.min_spike_periods
    if min_spike_periods is None:
        min_spike_periods = params.get("min_spike_periods", MIN_SPIKE_PERIODS)

    spark = (
        SparkSession.builder
        .appName("BuildSlangCandidatesFabric")
        .getOrCreate()
    )

    # -------------------------
    # Choose global vs subreddit inputs / outputs
    # -------------------------
    # -- Global mode:
    #       One candidate decision per token across the whole Reddit corpus.
    #       Key columns: [corpus, token]
    #
    # -- Subreddit mode:
    #       One candidate decision per token per subreddit.
    #       Key columns: [subreddit, token]
    # -------------------------
    clean_corpus_dir = config["paths"]["clean_corpus"]
    out_dir = config["paths"]["out_dir"]
    lexicons = config.get("lexicons", {})

    if args.mode == "global":
        clean_ratio_scores_dir = join_path(out_dir, "clean_ratio_scores_global_parquet")
        spike_scores_dir = join_path(out_dir, f"spike_scores_{args.spike_time_grain}_global_parquet")

        clean_ratio_candidates_parquet_dir = join_path(out_dir, "slang_candidates_clean_ratio_global_parquet")
        clean_ratio_candidates_csv_dir = join_path(out_dir, "slang_candidates_clean_ratio_global_csv")

        clean_ratio_spiking_candidates_parquet_dir = join_path(
            out_dir,
            f"slang_candidates_clean_ratio_spiking_{args.spike_time_grain}_global_parquet",
        )
        clean_ratio_spiking_candidates_csv_dir = join_path(
            out_dir,
            f"slang_candidates_clean_ratio_spiking_{args.spike_time_grain}_global_csv",
        )

        candidate_key_cols = ["corpus", "token"]

    else:
        clean_ratio_scores_dir = join_path(out_dir, "clean_ratio_scores_subreddit_parquet")
        spike_scores_dir = join_path(out_dir, f"spike_scores_{args.spike_time_grain}_subreddit_parquet")

        clean_ratio_candidates_parquet_dir = join_path(out_dir, "slang_candidates_clean_ratio_subreddit_parquet")
        clean_ratio_candidates_csv_dir = join_path(out_dir, "slang_candidates_clean_ratio_subreddit_csv")

        clean_ratio_spiking_candidates_parquet_dir = join_path(
            out_dir,
            f"slang_candidates_clean_ratio_spiking_{args.spike_time_grain}_subreddit_parquet",
        )
        clean_ratio_spiking_candidates_csv_dir = join_path(
            out_dir,
            f"slang_candidates_clean_ratio_spiking_{args.spike_time_grain}_subreddit_csv",
        )

        candidate_key_cols = ["subreddit", "token"]

    clean_ratio_scores = spark.read.parquet(clean_ratio_scores_dir)
    spike_scores = spark.read.parquet(spike_scores_dir)

    # -------------------------
    # Build broad slang candidates from clean-ratio scores
    # -------------------------
    # -- This table answers:
    #       "Does this token look slang-like because it is much more common
    #        in Reddit than in the clean Wikipedia corpus?"
    #
    # -- Output grain:
    #       global mode:    one row per [corpus, token]
    #       subreddit mode: one row per [subreddit, token]
    #
    # -- Renamed columns:
    #       count_reddit_all_time / total_tokens_all_time / p_reddit_all_time
    #       make it explicit that these values come from all-time clean-ratio
    #       scoring, not from a single spike week.
    # -------------------------
    clean_ratio_candidates = (
        clean_ratio_scores
        .filter(col("count_reddit") >= min_token_count)
        .filter(col("clean_ratio_score") >= min_clean_ratio_score)
        .select(
            *candidate_key_cols,
            col("count_reddit").alias("count_reddit_all_time"),
            col("total_tokens").alias("total_tokens_all_time"),
            col("p_reddit").alias("p_reddit_all_time"),
            "count_clean",
            "total_clean_tokens",
            "p_clean",
            "clean_ratio_score",
            lit(True).alias("is_slang_candidate_clean_ratio"),
        )
        .distinct()
    )

    # -------------------------
    # Filter out profanity / typos
    # -------------------------
    profanity = (
        spark.read.text(lexicons.get("profanity", join_path(clean_corpus_dir, "lexicons/profanity.txt")))
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_profane", lit(True))
    )

    typos = (
        spark.read.text(lexicons.get("typos", join_path(clean_corpus_dir, "lexicons/typos.txt")))
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_typo", lit(True))
    )

    reddit_jargon = (
        spark.read.text(lexicons.get("reddit_jargon", join_path(clean_corpus_dir, "lexicons/reddit_jargon.txt")))
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_reddit_jargon", lit(True))
    )

    clean_ratio_candidates = (
        clean_ratio_candidates
        .join(profanity, on="token", how="left")
        .withColumn("is_profane", coalesce(col("is_profane"), lit(False)))
        .filter(~col("is_profane"))
    )

    clean_ratio_candidates = (
        clean_ratio_candidates
        .join(typos, on="token", how="left")
        .withColumn("is_typo", coalesce(col("is_typo"), lit(False)))
        .filter(~col("is_typo"))
    )

    clean_ratio_candidates = (
        clean_ratio_candidates
        .join(reddit_jargon, on="token", how="left")
        .withColumn("is_reddit_jargon", coalesce(col("is_reddit_jargon"), lit(False)))
        .filter(~col("is_reddit_jargon"))
    )

    # -------------------------
    # Summarize spike evidence by candidate token
    # -------------------------
    # -- spike_scores is period-level:
    #       monthly: [token, year, month, period_id, spike_score, ...]
    #       yearly:  [token, year, period_id, spike_score, ...]
    #
    # -- For final slang decisions, we only need to know whether the token
    #    ever spiked strongly enough, not whether each occurrence happened
    #    inside a spike period.
    #
    # -- Therefore we collapse period-level spike rows to token-level evidence:
    #       max_spike_score:
    #           Strongest spike observed for the token.
    #       num_spike_periods:
    #           Number of distinct monthly/yearly periods where spike_score
    #           passes threshold.
    # -------------------------
    spike_evidence = (
        spike_scores
        .filter(col("spike_score") >= min_spike_score)
        .groupBy(*candidate_key_cols)
        .agg(
            spark_max("spike_score").alias("max_spike_score"),
            countDistinct("period_id").alias("num_spike_periods"),
        )
        .filter(col("num_spike_periods") >= min_spike_periods)
    )

    # -------------------------
    # Build stricter clean-ratio + spiking candidates
    # -------------------------
    # -- This table answers:
    #       "Does this token already pass the clean-ratio slang filter, and
    #        did it also spike into popularity at least once?"
    #
    # -- This is intentionally a second pass over clean-ratio candidates.
    #    A spike alone is not enough to label a token as slang because ordinary
    #    event/news words can spike too.
    # -------------------------
    clean_ratio_spiking_candidates = (
        clean_ratio_candidates
        .join(spike_evidence, on=candidate_key_cols, how="inner")
        .withColumn("is_slang_candidate_clean_ratio_spiking", lit(True))
    )

    # -------------------------
    # Write clean-ratio slang candidates
    # -------------------------
    clean_ratio_candidates.write.mode("overwrite").parquet(
        clean_ratio_candidates_parquet_dir
    )

    (
        clean_ratio_candidates
        .orderBy(col("clean_ratio_score").desc())
        .limit(1000)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(clean_ratio_candidates_csv_dir)
    )

    # -------------------------
    # Write clean-ratio + spiking slang candidates
    # -------------------------
    clean_ratio_spiking_candidates.write.mode("overwrite").parquet(
        clean_ratio_spiking_candidates_parquet_dir
    )

    (
        clean_ratio_spiking_candidates
        .orderBy(col("max_spike_score").desc(), col("clean_ratio_score").desc())
        .limit(1000)
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(clean_ratio_spiking_candidates_csv_dir)
    )

    # -------------------------
    # Console previews
    # -------------------------
    print("Clean-ratio slang candidates:")
    clean_ratio_candidates.orderBy(col("clean_ratio_score").desc()).show(
        50,
        truncate=False,
    )

    print("Clean-ratio + spiking slang candidates:")
    clean_ratio_spiking_candidates.orderBy(
        col("max_spike_score").desc(),
        col("clean_ratio_score").desc(),
    ).show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
