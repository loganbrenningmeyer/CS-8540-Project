# Subset BERT Input Tables

This note documents the recommended three-table distribution format for the
sampled Reddit subset:

- `reddit_clean_posts_subset_10pct_parquet`
- `reddit_tokens_subset_10pct_parquet`
- `slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet`

These three tables are enough to:

- reconstruct the standard BERT-input rows for positive slang candidates
- recover non-candidate token occurrences without re-tokenizing raw text
- keep storage smaller than a single giant token-plus-full-text export

## Table 1: `reddit_clean_posts_subset_10pct_parquet`

Produced by:

- `spark/fabric/reddit_clean_posts_subset.py`

Purpose:

- one row per sampled post/comment
- canonical source of full cleaned text for the sampled subset

How it is built:

- take distinct `post_id` values from `reddit_tokens_subset_10pct_parquet`
- inner join those ids to `reddit_clean_posts_parquet`

Guaranteed fields:

| Field | Type | Description |
| --- | --- | --- |
| `post_id` | string | Reddit post/comment id used as the primary join key. |

Expected additional fields:

- whatever columns already exist in `reddit_clean_posts_parquet`
- in practice this should include at least:
  - `subreddit`
  - `timestamp`
  - `body`

Storage role:

- stores full text once per post instead of once per token occurrence
- should be treated as the authoritative text table for the sampled subset

## Table 2: `reddit_tokens_subset_10pct_parquet`

Produced by:

- `spark/fabric/reddit_tokens_subset.py`

Purpose:

- one row per token occurrence in the sampled subset
- canonical sparse all-token table for the subset

Expected fields:

| Field | Type | Description |
| --- | --- | --- |
| `post_id` | string | Parent Reddit post/comment id. |
| `subreddit` | string | Subreddit inherited from the parent post/comment. |
| `timestamp` | timestamp | Timestamp inherited from the parent post/comment. |
| `year` | integer | Calendar year bucket. |
| `month` | integer | Calendar month bucket. |
| `week_start_date` | date | Weekly bucket. |
| `token` | string | Normalized token text. |
| `surface` | string | Exact matched surface form from the source text. |
| `token_start` | integer | Character start offset of `surface` in the source text. |
| `token_end` | integer | Character end offset of `surface` in the source text. |

Storage role:

- all tokens, not just slang candidates
- sparse token/offset representation with no duplicated full text
- supports recovery of non-candidate tokens by anti-join

## Table 3: `slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet`

Produced by:

- `spark/fabric/slang_occurrences_combined_spiking.py`

Purpose:

- one row per matched slang-candidate token occurrence
- sparse positive-occurrence table for the combined monthly/yearly spiking
  candidate set

Guaranteed fields from the script:

| Field | Type | Description |
| --- | --- | --- |
| `post_id` | string | Parent Reddit post/comment id. |
| `subreddit` | string | Subreddit of the matched occurrence. |
| `timestamp` | timestamp | Timestamp inherited from the token occurrence. |
| `year` | integer | Year bucket inherited from the token occurrence. |
| `month` | integer | Month bucket inherited from the token occurrence. |
| `week_start_date` | date | Weekly bucket inherited from the token occurrence. |
| `token` | string | Normalized candidate token. |
| `surface` | string | Exact matched surface form from the post text. |
| `token_start` | integer | Character start offset of `surface` in the source text. |
| `token_end` | integer | Character end offset of `surface` in the source text. |
| `is_slang_candidate` | boolean | Always `true` in this table. |
| `candidate_type` | string | Set to `spiking_combined`. |
| `candidate_mode` | string | Usually `subreddit`. |

Expected candidate metadata columns:

- `count_reddit_all_time`
- `total_tokens_all_time`
- `p_reddit_all_time`
- `count_clean`
- `total_clean_tokens`
- `p_clean`
- `clean_ratio_score`
- `has_monthly_spike`
- `max_spike_score_monthly`
- `num_spike_periods_monthly`
- `has_yearly_spike`
- `max_spike_score_yearly`
- `num_spike_periods_yearly`
- `spike_profile`

Storage role:

- compact positive-candidate occurrence table
- no duplicated full text
- no non-candidate token occurrences

## Standard BERT Input Format

The standard BERT-input rows are:

- `post_id`
- `subreddit`
- `timestamp`
- `token`
- `token_start`
- `token_end`
- `full_text`
- `is_slang_candidate`

## How To Reconstruct Positive Candidate BERT Inputs

If you only want BERT rows for the slang candidates, join:

- `slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet`
- `reddit_clean_posts_subset_10pct_parquet`

on `post_id`.

### PySpark example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

occ = spark.read.parquet(
    "hdfs:///outputs/slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet"
)

posts = spark.read.parquet(
    "hdfs:///outputs/reddit_clean_posts_subset_10pct_parquet"
)

bert_inputs = (
    occ
    .join(
        posts.select("post_id", col("body").alias("full_text")),
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
        "is_slang_candidate",
    )
)
```

This is lossless for positive candidate occurrences because:

- the occurrence table already has token identity and offsets
- the clean-post subset provides the original cleaned text

## How To Reconstruct All-Token BERT Inputs

If you want BERT-style rows for all tokens in the subset, use all three tables:

- `reddit_tokens_subset_10pct_parquet`
- `slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet`
- `reddit_clean_posts_subset_10pct_parquet`

The pattern is:

1. start from `reddit_tokens_subset_10pct_parquet`
2. left join candidate occurrence keys to mark positive slang candidates
3. join to `reddit_clean_posts_subset_10pct_parquet` on `post_id` to get
   `full_text`

### PySpark example

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

spark = SparkSession.builder.getOrCreate()

tokens = spark.read.parquet(
    "hdfs:///outputs/reddit_tokens_subset_10pct_parquet"
)

occ = spark.read.parquet(
    "hdfs:///outputs/slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet"
)

posts = spark.read.parquet(
    "hdfs:///outputs/reddit_clean_posts_subset_10pct_parquet"
)

candidate_keys = (
    occ
    .select(
        "post_id",
        "token",
        "token_start",
        "token_end",
        "is_slang_candidate",
    )
    .distinct()
)

bert_inputs_all = (
    tokens
    .join(
        candidate_keys,
        on=["post_id", "token", "token_start", "token_end"],
        how="left",
    )
    .join(
        posts.select("post_id", col("body").alias("full_text")),
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
        coalesce(col("is_slang_candidate"), lit(False)).alias(
            "is_slang_candidate"
        ),
    )
)
```

This is the best compact distribution format if a downstream user may need both:

- positive candidate occurrences
- non-candidate token occurrences

## Do You Need To Re-Tokenize To Get Non-Candidates?

Not if all three tables are distributed.

With the three-table layout, non-candidate tokens can be recovered directly from
`reddit_tokens_subset_10pct_parquet` by excluding the positive occurrences in
`slang_occurrences_clean_ratio_spiking_combined_subreddit_parquet`.

### Non-candidate recovery

```python
non_candidates = (
    tokens
    .join(
        occ.select(
            "post_id",
            "token",
            "token_start",
            "token_end",
        ).distinct(),
        on=["post_id", "token", "token_start", "token_end"],
        how="left_anti",
    )
)
```

That is much better than re-tokenizing the post text from scratch.
