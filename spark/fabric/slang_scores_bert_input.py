import argparse
import math

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    exp,
    greatest,
    least,
    lit,
    log1p,
    max as spark_max,
    row_number,
    when,
)
from pyspark.sql.window import Window

from utils.config import load_config
from utils.paths import join_path


DEFAULT_CLEAN_RATIO_C = 3.0
DEFAULT_CLEAN_RATIO_S = 0.4
DEFAULT_SPIKE_MONTHLY_C = 2.3
DEFAULT_SPIKE_MONTHLY_S = 0.4
DEFAULT_SPIKE_YEARLY_C = 2.3
DEFAULT_SPIKE_YEARLY_S = 0.4
DEFAULT_RELIABILITY_LOG = 101.0
DEFAULT_SPIKE_BOOST_WEIGHT = 0.6
DEFAULT_POWER_TRANSFORM = 1.0
DEFAULT_RANK_TOP_K = 0
DEFAULT_RANK_TOP_K_FLOOR = 0.8
DEFAULT_TAIL_DECAY = 0.002


def sigmoid(expr):
    return lit(1.0) / (lit(1.0) + exp(-expr))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--mode", choices=["global", "subreddit"], default="subreddit")
    parser.add_argument(
        "--output-table",
        default=None,
        help="Optional output parquet table name. Defaults to a mode-specific token-level slang-score table.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    score_params = config.get("score_params", {})

    clean_ratio_c = score_params.get("clean_ratio_c", DEFAULT_CLEAN_RATIO_C)
    clean_ratio_s = score_params.get("clean_ratio_s", DEFAULT_CLEAN_RATIO_S)
    spike_monthly_c = score_params.get("spike_monthly_c", DEFAULT_SPIKE_MONTHLY_C)
    spike_monthly_s = score_params.get("spike_monthly_s", DEFAULT_SPIKE_MONTHLY_S)
    spike_yearly_c = score_params.get("spike_yearly_c", DEFAULT_SPIKE_YEARLY_C)
    spike_yearly_s = score_params.get("spike_yearly_s", DEFAULT_SPIKE_YEARLY_S)
    reliability_log = score_params.get("reliability_log", DEFAULT_RELIABILITY_LOG)
    spike_boost_weight = score_params.get(
        "spike_boost_weight",
        DEFAULT_SPIKE_BOOST_WEIGHT,
    )
    power_transform = score_params.get(
        "power_transform",
        DEFAULT_POWER_TRANSFORM,
    )
    rank_top_k = score_params.get("rank_top_k", DEFAULT_RANK_TOP_K)
    rank_top_k_floor = score_params.get(
        "rank_top_k_floor",
        DEFAULT_RANK_TOP_K_FLOOR,
    )
    tail_decay = score_params.get("tail_decay", DEFAULT_TAIL_DECAY)

    if clean_ratio_s <= 0:
        raise ValueError("score_params.clean_ratio_s must be > 0")
    if spike_monthly_s <= 0:
        raise ValueError("score_params.spike_monthly_s must be > 0")
    if spike_yearly_s <= 0:
        raise ValueError("score_params.spike_yearly_s must be > 0")
    if reliability_log <= 1:
        raise ValueError("score_params.reliability_log must be > 1")
    if not 0 <= spike_boost_weight <= 1:
        raise ValueError("score_params.spike_boost_weight must be in [0, 1]")
    if power_transform <= 0:
        raise ValueError("score_params.power_transform must be > 0")
    if rank_top_k < 0:
        raise ValueError("score_params.rank_top_k must be >= 0")
    if not 0 < rank_top_k_floor <= 1:
        raise ValueError("score_params.rank_top_k_floor must be in (0, 1]")
    if tail_decay <= 0:
        raise ValueError("score_params.tail_decay must be > 0")

    spark = (
        SparkSession.builder
        .appName("BuildSlangScoresBertInputFabric")
        .getOrCreate()
    )

    clean_corpus_dir = config["paths"]["clean_corpus"]
    out_dir = config["paths"]["out_dir"]
    lexicons = config.get("lexicons", {})

    if args.mode == "global":
        clean_ratio_name = "clean_ratio_scores_global_parquet"
        monthly_spikes_name = "spike_scores_monthly_global_parquet"
        yearly_spikes_name = "spike_scores_yearly_global_parquet"
        output_name = "slang_scores_bert_input_global_parquet"
        score_key_cols = ["corpus", "token"]
        output_cols = ["token", "slang_score"]
    else:
        clean_ratio_name = "clean_ratio_scores_subreddit_parquet"
        monthly_spikes_name = "spike_scores_monthly_subreddit_parquet"
        yearly_spikes_name = "spike_scores_yearly_subreddit_parquet"
        output_name = "slang_scores_bert_input_subreddit_parquet"
        score_key_cols = ["subreddit", "token"]
        output_cols = ["subreddit", "token", "slang_score"]

    if args.output_table is not None:
        output_name = args.output_table

    clean_ratio_dir = join_path(out_dir, clean_ratio_name)
    monthly_spikes_dir = join_path(out_dir, monthly_spikes_name)
    yearly_spikes_dir = join_path(out_dir, yearly_spikes_name)
    output_dir = join_path(out_dir, output_name)

    clean_ratio_scores = spark.read.parquet(clean_ratio_dir)
    monthly_spike_scores = spark.read.parquet(monthly_spikes_dir)
    yearly_spike_scores = spark.read.parquet(yearly_spikes_dir)

    profanity = (
        spark.read.text(
            lexicons.get(
                "profanity",
                join_path(clean_corpus_dir, "lexicons/profanity.txt"),
            )
        )
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_profane", lit(True))
    )

    typos = (
        spark.read.text(
            lexicons.get(
                "typos",
                join_path(clean_corpus_dir, "lexicons/typos.txt"),
            )
        )
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_typo", lit(True))
    )

    reddit_jargon = (
        spark.read.text(
            lexicons.get(
                "reddit_jargon",
                join_path(clean_corpus_dir, "lexicons/reddit_jargon.txt"),
            )
        )
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_reddit_jargon", lit(True))
    )

    subreddits = (
        spark.read.text(
            lexicons.get(
                "subreddits",
                join_path(clean_corpus_dir, "lexicons/subreddits.txt"),
            )
        )
        .select(col("value").alias("token"))
        .filter(col("token") != "")
        .distinct()
        .withColumn("is_subreddit", lit(True))
    )

    clean_ratio_features = (
        clean_ratio_scores
        .select(
            *score_key_cols,
            col("count_reddit").alias("count_reddit_all_time"),
            "clean_ratio_score",
        )
        .join(profanity, on="token", how="left")
        .withColumn("is_profane", coalesce(col("is_profane"), lit(False)))
        .filter(~col("is_profane"))
        .join(typos, on="token", how="left")
        .withColumn("is_typo", coalesce(col("is_typo"), lit(False)))
        .filter(~col("is_typo"))
        .join(reddit_jargon, on="token", how="left")
        .withColumn(
            "is_reddit_jargon",
            coalesce(col("is_reddit_jargon"), lit(False)),
        )
        .filter(~col("is_reddit_jargon"))
        .join(subreddits, on="token", how="left")
        .withColumn("is_subreddit", coalesce(col("is_subreddit"), lit(False)))
        .filter(~col("is_subreddit"))
        .filter(
            ~col("token").rlike(r"(.)\1{4,}|^(.{1,3})\1{3,}$")
        )
        .filter(
            ~col("token").rlike(
                r"^(?:ha){3,}h?$|^(?:he){3,}h?$|^(?:lo){3,}l?$|^(?:um){2,}m?$|^(?:uh){2,}h?$|^(.)\1{3,}$|^(.{1,4})(?:\2){2,}.?$"
            )
        )
        .select(*score_key_cols, "count_reddit_all_time", "clean_ratio_score")
    )

    monthly_spike_features = (
        monthly_spike_scores
        .groupBy(*score_key_cols)
        .agg(
            spark_max("spike_score").alias("max_spike_score_monthly"),
        )
    )

    yearly_spike_features = (
        yearly_spike_scores
        .groupBy(*score_key_cols)
        .agg(
            spark_max("spike_score").alias("max_spike_score_yearly"),
        )
    )

    reliability_denominator = math.log(reliability_log)
    clean_weight = 1.0 - spike_boost_weight

    score_df = (
        clean_ratio_features
        .join(monthly_spike_features, on=score_key_cols, how="left")
        .join(yearly_spike_features, on=score_key_cols, how="left")
        .withColumn(
            "clean_component",
            sigmoid(
                (col("clean_ratio_score") - lit(clean_ratio_c))
                / lit(clean_ratio_s)
            ),
        )
        .withColumn(
            "monthly_component",
            sigmoid(
                (
                    coalesce(col("max_spike_score_monthly"), lit(0.0))
                    - lit(spike_monthly_c)
                )
                / lit(spike_monthly_s)
            ),
        )
        .withColumn(
            "yearly_component",
            sigmoid(
                (
                    coalesce(col("max_spike_score_yearly"), lit(0.0))
                    - lit(spike_yearly_c)
                )
                / lit(spike_yearly_s)
            ),
        )
        .withColumn(
            "spike_boost",
            greatest(col("monthly_component"), col("yearly_component")),
        )
        .withColumn(
            "reliability",
            least(
                lit(1.0),
                log1p(col("count_reddit_all_time")) / lit(reliability_denominator),
            ),
        )
        .withColumn(
            "slang_score_raw",
            col("reliability")
            * col("clean_component")
            * (
                lit(clean_weight)
                + lit(spike_boost_weight) * col("spike_boost")
            ),
        )
        .withColumn(
            "slang_score_power",
            col("slang_score_raw") ** lit(power_transform),
        )
    )

    if rank_top_k > 0:
        if args.mode == "global":
            rank_window = Window.orderBy(
                col("slang_score_power").desc(),
                col("token").asc(),
            )
        else:
            rank_window = Window.orderBy(
                col("slang_score_power").desc(),
                col("subreddit").asc(),
                col("token").asc(),
            )

        top_k_denominator = max(rank_top_k - 1, 1)

        score_df = (
            score_df
            .withColumn("score_rank", row_number().over(rank_window))
            .withColumn(
                "slang_score",
                when(
                    col("score_rank") <= lit(rank_top_k),
                    lit(rank_top_k_floor)
                    + (lit(1.0) - lit(rank_top_k_floor))
                    * (
                        lit(1.0)
                        - (
                            (col("score_rank") - lit(1.0))
                            / lit(float(top_k_denominator))
                        )
                    ),
                ).otherwise(
                    lit(rank_top_k_floor)
                    * exp(
                        -lit(tail_decay)
                        * (col("score_rank") - lit(float(rank_top_k)))
                    )
                ),
            )
        )
    else:
        score_df = score_df.withColumn("slang_score", col("slang_score_power"))

    bert_input_scores = score_df.select(*output_cols)

    bert_input_scores.write.mode("overwrite").parquet(output_dir)

    print("output_table={}".format(output_name))
    print("clean_ratio_c={}".format(clean_ratio_c))
    print("clean_ratio_s={}".format(clean_ratio_s))
    print("spike_monthly_c={}".format(spike_monthly_c))
    print("spike_monthly_s={}".format(spike_monthly_s))
    print("spike_yearly_c={}".format(spike_yearly_c))
    print("spike_yearly_s={}".format(spike_yearly_s))
    print("reliability_log={}".format(reliability_log))
    print("spike_boost_weight={}".format(spike_boost_weight))
    print("power_transform={}".format(power_transform))
    print("rank_top_k={}".format(rank_top_k))
    print("rank_top_k_floor={}".format(rank_top_k_floor))
    print("tail_decay={}".format(tail_decay))
    print("wrote_bert_input_scores={}".format(output_dir))

    spark.stop()


if __name__ == "__main__":
    main()
