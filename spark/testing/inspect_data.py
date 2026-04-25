import zstandard as zstd
import json
import io
from datetime import datetime, timezone
import re

# -------------------------
# Word-level Regex:
# - \b --> Start word boundary
# - [\w]+ --> One or more word characters
# - (?:[-'][\w]+)* --> Optionally capture repeated group: hyphenated or apostrophes (e.g., rock-and-roll, y'all'd've)
# - \b --> End word boundary
#
# Allows capturing words like:
# - lmaooo!! -> lmaooo
# - foo_bar -> foo_bar
# - don't -> don't
# - y'all -> y'all 
# - hi!hello -> hi, hello
# - rock-and-roll -> rock-and-roll
# - low-key -> low-key
# -------------------------
TOKEN_RE = re.compile(r"\b[\w]+(?:[-'][\w]+)*\b", re.UNICODE)

def tokenize_with_offsets(text: str):
    for m in TOKEN_RE.finditer(text):
        yield {
            "token": m.group(0).lower(),
            "surface": m.group(0),
            "is_upper": m.group().isupper(),
            "token_start": m.start(),
            "token_end": m.end(),
        }

def print_tokenized_body(text: str):
    for item in tokenize_with_offsets(text):
        print(
            f"\ttoken={item['token']}, "
            f"surface={item['surface']}, "
            f"is_upper={item['is_upper']} "
            f"start={item['token_start']}, "
            f"end={item['token_end']}"
        )

def print_line(data: dict):
    timestamp = datetime.fromtimestamp(int(data['created_utc']), tz=timezone.utc)

    print(str(data["id"]).center(50, "="))
    print(f"{data['subreddit']=}")
    print(f"data['created_utc']={timestamp}")
    print(f"{data['author']=}")
    print(f"{data['score']=}")
    print(f"{data['body']=}")
    print("tokenized body:")
    print_tokenized_body(data["body"])


def main():
    file_path = "/mnt/c/Users/logan/Downloads/Datasets/Reddit/reddit/subreddits25/AskReddit_comments.zst"

    with open(file_path, "rb") as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            
            for i, line in enumerate(text_stream):
                data = json.loads(line)
                print_line(data)
                if i == 5:
                    break


if __name__ == "__main__":
    main()
