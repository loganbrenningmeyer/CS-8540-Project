# Subset BERT Input Tables For Slang Scores

Tables needed:

- `slang_scores_bert_input_subreddit_parquet`
- `reddit_tokens_subset_10pct_parquet`
- `reddit_clean_posts_subset_10pct_parquet`

`slang_scores_bert_input_subreddit_parquet` fields:

- `subreddit`
- `token`
- `slang_score`

Final BERT input fields:

- `post_id`
- `subreddit`
- `timestamp`
- `token`
- `token_start`
- `token_end`
- `full_text`
- `slang_score`

Build order:

1. Left join `reddit_tokens_subset_10pct_parquet` to `slang_scores_bert_input_subreddit_parquet` on `(subreddit, token)`.
2. Join the result to `reddit_clean_posts_subset_10pct_parquet` on `post_id`.
3. Rename `body` to `full_text`.
4. Select the final BERT input fields.

PySpark example:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, lit

spark = SparkSession.builder.getOrCreate()

tokens = spark.read.parquet(
    "hdfs:///outputs/reddit_tokens_subset_10pct_parquet"
)

scores = spark.read.parquet(
    "hdfs:///outputs/slang_scores_bert_input_subreddit_parquet"
)

posts = spark.read.parquet(
    "hdfs:///outputs/reddit_clean_posts_subset_10pct_parquet"
)

bert_inputs = (
    tokens
    .join(scores, on=["subreddit", "token"], how="left")
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
        coalesce(col("slang_score"), lit(0.0)).alias("slang_score"),
    )
)
```
