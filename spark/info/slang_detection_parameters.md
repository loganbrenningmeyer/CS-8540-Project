# Slang Detection Parameters

This file describes the main parameters used by the slang-detection pipeline and gives recommended starting settings.

## Main Strategy

The current pipeline uses three kinds of evidence:

1. Clean-ratio evidence: the token is much more common in Reddit than in Wikipedia.
2. Spike evidence: the token rises above its recent monthly or yearly baseline.
3. Exclusion lexicons: profanity, typos, and Reddit platform terms are filtered out.

The broad slang candidate rule should mainly come from clean-ratio evidence. Spike evidence should be treated as a stricter subset for emerging or time-sensitive slang.

## `reddit_stats.py`

`reddit_stats.py` builds the frequency tables used by later scoring scripts.

### `filter_candidate_tokens(..., allow_numbers=False)`

Controls which tokens are counted as candidate words in `count_reddit`.

Current behavior:

```text
minimum length >= 3
no underscores
no numbers
letters with optional hyphen/apostrophe connectors
```

Important detail: `total_tokens` is still computed from unfiltered Reddit tokens. This keeps denominators honest while restricting candidate token rows.

Recommended:

```python
allow_numbers=False
```

This removes many IDs, dates, scores, and noisy alphanumeric tokens.

## `clean_ratio_scores.py`

Clean-ratio score:

```text
clean_ratio_score = log(p_reddit / p_clean)
```

Interpretation:

```text
0.0  = equally common in Reddit and Wikipedia
1.6  = about 5x more common in Reddit
2.3  = about 10x more common in Reddit
3.0  = about 20x more common in Reddit
```

Recommended use:

```text
Do not make final slang decisions in clean_ratio_scores.py.
Use this script to compute evidence only.
```

## `spike_scores.py`

`spike_scores.py` computes monthly and/or yearly spike evidence for broad clean-ratio candidates.

### `--mode`

Controls whether scores are computed globally or per subreddit.

```text
subreddit = one score per subreddit-token-period
global    = one score per token-period across all subreddits
```

Recommended:

```bash
--mode subreddit
```

Use subreddit mode when you care about community-specific slang or jargon.

### `--time-grain`

Controls the temporal unit for spike scoring.

```text
monthly = smoother emergence/adoption signal
yearly  = broad historical trend signal
both    = write both monthly and yearly outputs
```

Recommended:

```bash
--time-grain monthly
```

Use yearly as a secondary analysis when you want longer-term historical change.

### `--min-token-count`

Minimum all-time Reddit count needed before spike evidence is computed for a token.

This is a broad prefilter for performance and reliability.

Recommended:

```bash
--min-token-count 100
```

For small local samples:

```bash
--min-token-count 20
```

### `--min-clean-ratio-score`

Minimum clean-ratio score needed before spike evidence is computed.

Recommended:

```bash
--min-clean-ratio-score 0.0
```

This means the token is at least as common in Reddit as Wikipedia. Keep this broad because final slang filtering happens in `slang_candidates.py`.

### `--monthly-window-periods`

Number of previous months used to build the monthly baseline.

Recommended:

```bash
--monthly-window-periods 6
```

This is long enough to smooth short-term noise but short enough to detect adoption within a year.

### `--yearly-window-periods`

Number of previous years used to build the yearly baseline.

Recommended:

```bash
--yearly-window-periods 3
```

This gives a stable multi-year historical baseline.

### `--min-baseline-periods`

Minimum number of previous active periods required before scoring a spike.

Recommended monthly:

```bash
--min-baseline-periods 3
```

Recommended yearly:

```bash
--min-baseline-periods 2
```

This prevents scoring a spike when there is not enough history.

### `--min-period-count`

Minimum current-period token count required to keep a spike row.

Recommended monthly:

```bash
--min-period-count 10
```

Recommended yearly:

```bash
--min-period-count 25
```

For small local samples:

```bash
--min-period-count 2
```

This prevents one or two mentions from creating a large spike score.

### `--min-baseline-total-tokens`

Minimum total token volume in the baseline window.

Recommended monthly:

```bash
--min-baseline-total-tokens 25000
```

Recommended yearly:

```bash
--min-baseline-total-tokens 100000
```

For small local samples:

```bash
--min-baseline-total-tokens 1000
```

This prevents small subreddits or sparse periods from producing unstable spike scores.

### `--smoothing-alpha`

Additive smoothing pseudo-count used in the spike ratio:

```text
p_current_smoothed = (count_reddit + alpha) / (total_tokens + alpha)
p_baseline_smoothed = (baseline_count_reddit + alpha) / (baseline_total_tokens + alpha)
spike_score = log(p_current_smoothed / p_baseline_smoothed)
```

Recommended:

```bash
--smoothing-alpha 1.0
```

Higher values make spike scores more conservative. Lower values make first appearances and rare tokens more sensitive.

## `slang_candidates.py`

`slang_candidates.py` makes final token-level candidate decisions.

### `--min-token-count`

Minimum all-time Reddit count for final slang candidates.

Recommended:

```bash
--min-token-count 100
```

For local samples:

```bash
--min-token-count 20
```

### `--min-clean-ratio-score`

Minimum clean-ratio score for the broad slang candidate table.

Recommended:

```bash
--min-clean-ratio-score 3.0
```

This means the token is about 20x more common in Reddit than Wikipedia. It is stricter than 2.3 and helps reduce general informal words.

### `--spike-time-grain`

Which spike table to use for the stricter spiking slang candidate table.

Recommended:

```bash
--spike-time-grain monthly
```

Use yearly for broader historical trend analysis.

### `--min-spike-score`

Minimum spike score for the stricter clean-ratio + spiking candidate table.

Interpretation:

```text
1.6 = about 5x over baseline
2.3 = about 10x over baseline
3.0 = about 20x over baseline
```

Recommended monthly:

```bash
--min-spike-score 2.3
```

Recommended yearly:

```bash
--min-spike-score 1.6
```

Yearly spikes are broader and slower-moving, so a 5x rise is already meaningful.

### `--min-spike-periods`

Minimum number of monthly/yearly periods where the token must pass the spike score threshold.

Recommended:

```bash
--min-spike-periods 1
```

Use `2` if you want sustained spikes rather than one-period emergence.

## Recommended Commands

### Monthly Main Pipeline

```bash
python spark/local/reddit_stats.py
python spark/local/clean_ratio_scores.py --mode subreddit
python spark/local/spike_scores.py \
  --mode subreddit \
  --time-grain monthly \
  --monthly-window-periods 6 \
  --min-baseline-periods 3 \
  --min-period-count 10 \
  --min-baseline-total-tokens 25000 \
  --smoothing-alpha 1.0
python spark/local/slang_candidates.py \
  --mode subreddit \
  --spike-time-grain monthly \
  --min-token-count 100 \
  --min-clean-ratio-score 3.0 \
  --min-spike-score 2.3 \
  --min-spike-periods 1
```

### Yearly Historical Pipeline

```bash
python spark/local/spike_scores.py \
  --mode subreddit \
  --time-grain yearly \
  --yearly-window-periods 3 \
  --min-baseline-periods 2 \
  --min-period-count 25 \
  --min-baseline-total-tokens 100000 \
  --smoothing-alpha 1.0
python spark/local/slang_candidates.py \
  --mode subreddit \
  --spike-time-grain yearly \
  --min-token-count 100 \
  --min-clean-ratio-score 3.0 \
  --min-spike-score 1.6 \
  --min-spike-periods 1
```

## Final Output Interpretation

Use:

```text
slang_candidates_clean_ratio_subreddit_parquet
```

as the broad slang candidate set.

Use:

```text
slang_candidates_clean_ratio_spiking_monthly_subreddit_parquet
```

as the stricter emerging slang candidate set.

Use:

```text
slang_candidates_clean_ratio_spiking_yearly_subreddit_parquet
```

for broader historical adoption analysis.
