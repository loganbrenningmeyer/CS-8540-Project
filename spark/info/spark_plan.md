For the BERT section, I need the Spark output in Parquet format 

Each record should have these fields:

- post_id
- subreddit
- timestamp
- token (the candidate word)
- token_start / token_end (character offsets in the original text)
- full_text (the full comment/sentence the token came from)
- is_slang_candidate (bool) - whether the token survived the clean corpus filter 

Also, BERT needs the surrounding context to make accurate classifications, so I'll just handle the windowing in Ray


# Spark Analysis Pipeline

1. `reddit_clean_posts.py`
2. `reddit_tokens.py`
3. `reddit_stats.py` & `clean_stats.py`
4. `clean_ratio_scores.py`
5. `spike_scores.py`
6. `slang_candidates.py`