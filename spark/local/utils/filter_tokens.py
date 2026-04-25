from pyspark.sql.functions import col, length


def filter_candidate_tokens(
    df,
    token_col="token",
    min_len=3,
    alphabetic_only=False,
    allow_hyphen=True,
    allow_apostrophe=True,
    allow_underscore=False,
    allow_numbers=True,
):
    out = df.filter(col(token_col).isNotNull())

    # Length filter
    if min_len is not None:
        out = out.filter(length(col(token_col)) >= min_len)

    # Explicit character exclusions (independent of regex)
    if not allow_underscore:
        out = out.filter(~col(token_col).contains("_"))

    if not allow_numbers:
        out = out.filter(~col(token_col).rlike(r"[0-9]"))

    # Build regex dynamically
    if alphabetic_only:
        base = "a-z"
    else:
        base = "a-z0-9"

    # Build allowed connectors
    connectors = ""
    if allow_hyphen:
        connectors += "-"
    if allow_apostrophe:
        connectors += "'"

    if connectors:
        pattern = rf"^[{base}]+([{connectors}][{base}]+)*$"
    else:
        pattern = rf"^[{base}]+$"

    out = out.filter(col(token_col).rlike(pattern))

    return out

