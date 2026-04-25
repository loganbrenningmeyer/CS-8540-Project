# Parquet Tables

This document describes the Parquet datasets written by the local Spark pipeline. Paths are relative to `spark/outputs/` unless noted otherwise.

## `reddit_clean_posts_parquet`

Produced by `spark/local/reddit_clean_posts.py`.

One row per cleaned Reddit post/comment selected for the local run.

| Field | Type | Description |
| --- | --- | --- |
| `post_id` | string | Reddit item id from the raw `id` field. |
| `subreddit` | string | Subreddit the post/comment came from. |
| `timestamp` | timestamp | UTC timestamp derived from `created_utc`. |
| `year` | integer | Calendar year extracted from `timestamp`. |
| `month` | integer | Calendar month extracted from `timestamp`. |
| `week_start_date` | date | Start date of the Spark `date_trunc("week", timestamp)` bucket. |
| `body` | string | Original comment/post body after removing null, `[deleted]`, and `[removed]` rows. |
| `score` | integer | Reddit score cast to integer. |

Notes:

- Local runs may be sampled by week using `LOCAL_POST_LIMIT`.
- This is the canonical post text table. Token-level tables should generally join to this table by `post_id` when full text is needed.

## `reddit_tokens_parquet`

Produced by `spark/local/reddit_tokens.py`.

One row per token occurrence extracted from `reddit_clean_posts_parquet`.

| Field | Type | Description |
| --- | --- | --- |
| `post_id` | string | Parent Reddit post/comment id. |
| `subreddit` | string | Subreddit inherited from the post/comment. |
| `timestamp` | timestamp | Timestamp inherited from the post/comment. |
| `year` | integer | Year inherited from the post/comment. |
| `month` | integer | Month inherited from the post/comment. |
| `week_start_date` | date | Week bucket inherited from the post/comment. |
| `token` | string | Normalized lowercase token. |
| `surface` | string | Exact matched token text from the original body. |
| `token_start` | integer | Character start offset of `surface` in `body`. |
| `token_end` | integer | Character end offset of `surface` in `body`. |

Notes:

- Tokenization uses `\b[\w]+(?:[-'][\w]+)*\b`.
- `token_start` is inclusive and `token_end` is exclusive, following Python regex match offsets.
- The current local script writes a capped token sample via `tokens.limit(1000000)`.

## `clean_stats_parquet`

Produced by `spark/local/clean_stats.py`.

One row per token in the clean Wikipedia corpus.

| Field | Type | Description |
| --- | --- | --- |
| `token` | string | Normalized lowercase clean-corpus token. |
| `count_clean` | long | Number of times the token appears in the Wikipedia corpus. |
| `total_clean_tokens` | long | Total token count across the Wikipedia corpus. Repeated on every row. |
| `p_clean` | double | Clean-corpus token probability: `count_clean / total_clean_tokens`. |

Notes:

- Input is `spark/data/wikipedia_parquet`.
- This table is joined to Reddit all-time stats by `token` to compute clean-ratio scores.

## Reddit Stats Tables

Produced by `spark/local/reddit_stats.py`.

These tables contain Reddit token counts and probabilities. Token counts use candidate-filtered tokens, while `total_tokens` is computed from unfiltered Reddit tokens for the same bucket.

### `reddit_stats_monthly_subreddit_parquet`

One row per `(subreddit, year, month, token)`.

| Field | Type | Description |
| --- | --- | --- |
| `subreddit` | string | Subreddit bucket. |
| `year` | integer | Calendar year bucket. |
| `month` | integer | Calendar month bucket. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | Token count within the subreddit-month bucket. |
| `total_tokens` | long | Total unfiltered Reddit token count within the subreddit-month bucket. |
| `p_reddit` | double | Reddit token probability: `count_reddit / total_tokens`. |

### `reddit_stats_all_time_subreddit_parquet`

One row per `(subreddit, token)`.

| Field | Type | Description |
| --- | --- | --- |
| `subreddit` | string | Subreddit bucket. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | Token count within the subreddit across all sampled time. |
| `total_tokens` | long | Total unfiltered Reddit token count within the subreddit across all sampled time. |
| `p_reddit` | double | Reddit token probability: `count_reddit / total_tokens`. |

### `reddit_stats_monthly_global_parquet`

One row per `(corpus, year, month, token)`. `corpus` is currently the literal string `reddit`.

| Field | Type | Description |
| --- | --- | --- |
| `corpus` | string | Corpus label. Currently `reddit`. |
| `year` | integer | Calendar year bucket. |
| `month` | integer | Calendar month bucket. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | Token count across all subreddits in the month. |
| `total_tokens` | long | Total unfiltered Reddit token count across all subreddits in the month. |
| `p_reddit` | double | Reddit token probability: `count_reddit / total_tokens`. |

### `reddit_stats_all_time_global_parquet`

One row per `(corpus, token)`. `corpus` is currently the literal string `reddit`.

| Field | Type | Description |
| --- | --- | --- |
| `corpus` | string | Corpus label. Currently `reddit`. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | Token count across all subreddits and sampled time. |
| `total_tokens` | long | Total unfiltered Reddit token count across all subreddits and sampled time. |
| `p_reddit` | double | Reddit token probability: `count_reddit / total_tokens`. |

## Clean Ratio Score Tables

Produced by `spark/local/clean_ratio_scores.py`.

These tables join Reddit all-time stats to `clean_stats_parquet` by `token` and compute:

```text
clean_ratio_score = log(p_reddit / p_clean)
```

Missing `p_clean` values are filled with `1e-12`.

### `clean_ratio_scores_subreddit_parquet`

Produced with `--mode subreddit`. One row per `(subreddit, token)`.

| Field | Type | Description |
| --- | --- | --- |
| `subreddit` | string | Subreddit bucket. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | All-time token count in the subreddit. |
| `total_tokens` | long | All-time unfiltered token count in the subreddit. |
| `p_reddit` | double | All-time subreddit Reddit probability. |
| `count_clean` | long | Wikipedia token count, null if missing before fill. |
| `total_clean_tokens` | long | Total Wikipedia token count, null if token is absent from Wikipedia. |
| `p_clean` | double | Wikipedia probability, filled with `1e-12` when absent. |
| `clean_ratio_score` | double | Log ratio of Reddit probability to clean-corpus probability. |

### `clean_ratio_scores_global_parquet`

Produced with `--mode global`. One row per `(corpus, token)`.

| Field | Type | Description |
| --- | --- | --- |
| `corpus` | string | Corpus label. Currently `reddit`. |
| `token` | string | Candidate-filtered normalized token. |
| `count_reddit` | long | All-time token count across all subreddits. |
| `total_tokens` | long | All-time unfiltered token count across all subreddits. |
| `p_reddit` | double | All-time global Reddit probability. |
| `count_clean` | long | Wikipedia token count, null if missing before fill. |
| `total_clean_tokens` | long | Total Wikipedia token count, null if token is absent from Wikipedia. |
| `p_clean` | double | Wikipedia probability, filled with `1e-12` when absent. |
| `clean_ratio_score` | double | Log ratio of Reddit probability to clean-corpus probability. |

## Spike Score Tables

Produced by `spark/local/spike_scores.py`.

These tables identify broad clean-ratio candidates and compute monthly and yearly spike scores against previous chronological periods.

Outputs:

```text
spike_scores_monthly_subreddit_parquet
spike_scores_yearly_subreddit_parquet
spike_scores_monthly_global_parquet
spike_scores_yearly_global_parquet
```

Default baseline windows:

```text
monthly: previous 6 months
yearly: previous 3 years
```

Spike computation candidate filter:

```text
count_reddit >= 100
clean_ratio_score >= 0.0
```

The stricter final slang decision filter is applied later in `spark/local/slang_candidates.py`.

Spike baseline:

```text
p_baseline = sum(previous count_reddit) / sum(previous total_tokens)
```

Smoothed spike score:

```text
p_current_smoothed = (count_reddit + alpha) / (total_tokens + alpha)
p_baseline_smoothed = (baseline_count_reddit + alpha) / (baseline_total_tokens + alpha)
spike_score = log(p_current_smoothed / p_baseline_smoothed)
```

The default `alpha` is `1.0`.

Rows are kept only when:

```text
baseline_periods >= 3
count_reddit >= 5
baseline_total_tokens >= 10000
```

### `spike_scores_monthly_subreddit_parquet` / `spike_scores_yearly_subreddit_parquet`

Produced with `--mode subreddit`. Monthly rows are scored by `(subreddit, token, year, month)`. Yearly rows are scored by `(subreddit, token, year)`.

| Field | Type | Description |
| --- | --- | --- |
| `subreddit` | string | Subreddit bucket. |
| `token` | string | Candidate token. |
| `year` | integer | Year of the current period. |
| `month` | integer | Month of the current period. Present only in monthly outputs. |
| `period_id` | string | Stable period label, such as `2020-04` or `2020`. |
| `time_grain` | string | Either `monthly` or `yearly`. |
| `count_reddit` | long | Current-period token count in the subreddit. |
| `total_tokens` | long | Current-period total unfiltered token count in the subreddit. |
| `p_reddit` | double | Current-period Reddit probability. |
| `count_reddit_all_time` | long | All-time token count from clean-ratio candidate selection. |
| `p_reddit_all_time` | double | All-time Reddit probability from clean-ratio candidate selection. |
| `p_clean` | double | Clean-corpus probability from clean-ratio scores. |
| `clean_ratio_score` | double | Clean-ratio score from candidate selection. |
| `baseline_count_reddit` | long | Sum of token counts over the previous window. |
| `baseline_total_tokens` | long | Sum of total token counts over the previous window. |
| `baseline_periods` | long | Number of prior periods in the window with `total_tokens > 0`. |
| `p_baseline` | double | Weighted previous-window baseline probability. |
| `p_current_smoothed` | double | Additive-smoothed current-period token probability. |
| `p_baseline_smoothed` | double | Additive-smoothed previous-window baseline probability. |
| `spike_score` | double | Log ratio of smoothed current probability to smoothed baseline probability. |

### `spike_scores_monthly_global_parquet` / `spike_scores_yearly_global_parquet`

Produced with `--mode global`. Monthly rows are scored by `(corpus, token, year, month)`. Yearly rows are scored by `(corpus, token, year)`.

| Field | Type | Description |
| --- | --- | --- |
| `corpus` | string | Corpus label. Currently `reddit`. |
| `token` | string | Candidate token. |
| `year` | integer | Year of the current period. |
| `month` | integer | Month of the current period. Present only in monthly outputs. |
| `period_id` | string | Stable period label, such as `2020-04` or `2020`. |
| `time_grain` | string | Either `monthly` or `yearly`. |
| `count_reddit` | long | Current-period token count across all subreddits. |
| `total_tokens` | long | Current-period total unfiltered token count across all subreddits. |
| `p_reddit` | double | Current-period global Reddit probability. |
| `count_reddit_all_time` | long | All-time token count from clean-ratio candidate selection. |
| `p_reddit_all_time` | double | All-time Reddit probability from clean-ratio candidate selection. |
| `p_clean` | double | Clean-corpus probability from clean-ratio scores. |
| `clean_ratio_score` | double | Clean-ratio score from candidate selection. |
| `baseline_count_reddit` | long | Sum of token counts over the previous window. |
| `baseline_total_tokens` | long | Sum of total token counts over the previous window. |
| `baseline_periods` | long | Number of prior periods in the window with `total_tokens > 0`. |
| `p_baseline` | double | Weighted previous-window baseline probability. |
| `p_current_smoothed` | double | Additive-smoothed current-period token probability. |
| `p_baseline_smoothed` | double | Additive-smoothed previous-window baseline probability. |
| `spike_score` | double | Log ratio of smoothed current probability to smoothed baseline probability. |

## Slang Candidate Tables

Produced by `spark/local/slang_candidates.py`.

These are token-level decision tables, not occurrence tables. Join them back to `reddit_tokens_parquet` by `(subreddit, token)` in subreddit mode or `(token)` / `(corpus, token)` in global mode to label token occurrences.

Default clean-ratio candidate filter:

```text
count_reddit >= 100
clean_ratio_score >= 2.3
```

Default spiking second-pass filter:

```text
spike_score >= 1.6
num_spike_periods >= 1
```

### `slang_candidates_clean_ratio_subreddit_parquet`

Produced with `--mode subreddit`. One row per `(subreddit, token)` that passes the clean-ratio slang filter.

| Field | Type | Description |
| --- | --- | --- |
| `subreddit` | string | Subreddit bucket. |
| `token` | string | Candidate token. |
| `count_reddit_all_time` | long | All-time token count in the subreddit. |
| `total_tokens_all_time` | long | All-time unfiltered token count in the subreddit. |
| `p_reddit_all_time` | double | All-time subreddit Reddit probability. |
| `count_clean` | long | Wikipedia token count, null if missing before fill. |
| `total_clean_tokens` | long | Total Wikipedia token count, null if token is absent from Wikipedia. |
| `p_clean` | double | Wikipedia probability used for clean-ratio scoring. |
| `clean_ratio_score` | double | Log ratio of Reddit probability to clean-corpus probability. |
| `is_slang_candidate_clean_ratio` | boolean | Always true for rows in this table. |

### `slang_candidates_clean_ratio_global_parquet`

Produced with `--mode global`. One row per `(corpus, token)` that passes the clean-ratio slang filter.

| Field | Type | Description |
| --- | --- | --- |
| `corpus` | string | Corpus label. Currently `reddit`. |
| `token` | string | Candidate token. |
| `count_reddit_all_time` | long | All-time token count across all subreddits. |
| `total_tokens_all_time` | long | All-time unfiltered token count across all subreddits. |
| `p_reddit_all_time` | double | All-time global Reddit probability. |
| `count_clean` | long | Wikipedia token count, null if missing before fill. |
| `total_clean_tokens` | long | Total Wikipedia token count, null if token is absent from Wikipedia. |
| `p_clean` | double | Wikipedia probability used for clean-ratio scoring. |
| `clean_ratio_score` | double | Log ratio of Reddit probability to clean-corpus probability. |
| `is_slang_candidate_clean_ratio` | boolean | Always true for rows in this table. |

### `slang_candidates_clean_ratio_spiking_monthly_subreddit_parquet` / `slang_candidates_clean_ratio_spiking_yearly_subreddit_parquet`

Produced with `--mode subreddit`. One row per `(subreddit, token)` that passes the clean-ratio filter and also has spike evidence.

This table contains all fields from `slang_candidates_clean_ratio_subreddit_parquet`, plus:

| Field | Type | Description |
| --- | --- | --- |
| `max_spike_score` | double | Strongest monthly/yearly spike score observed for this subreddit-token after applying the spike threshold. |
| `num_spike_periods` | long | Number of distinct periods where this subreddit-token passes the spike threshold. |
| `is_slang_candidate_clean_ratio_spiking` | boolean | Always true for rows in this table. |

### `slang_candidates_clean_ratio_spiking_monthly_global_parquet` / `slang_candidates_clean_ratio_spiking_yearly_global_parquet`

Produced with `--mode global`. One row per `(corpus, token)` that passes the clean-ratio filter and also has spike evidence.

This table contains all fields from `slang_candidates_clean_ratio_global_parquet`, plus:

| Field | Type | Description |
| --- | --- | --- |
| `max_spike_score` | double | Strongest monthly/yearly spike score observed for this token after applying the spike threshold. |
| `num_spike_periods` | long | Number of distinct periods where this token passes the spike threshold. |
| `is_slang_candidate_clean_ratio_spiking` | boolean | Always true for rows in this table. |

For storage efficiency, keep these as token-level decision tables and join to `reddit_tokens_parquet` only when building token occurrence annotations.
