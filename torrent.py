input_file = "subreddits25_by_size.txt"
output_file = "subreddits_clean.txt"

seen = set()

with open(input_file, "r") as f, open(output_file, "w") as out:
    for line in f:
        if not line.strip():
            continue

        _, name = line.strip().split("\t", 1)

        # normalize
        name = name.replace("reddit/subreddits25/", "")
        name = name.replace("_comments.zst", "")
        name = name.replace("_submissions.zst", "")
        name = name.lower()

        # deduplicate, keep first (largest)
        if name not in seen:
            seen.add(name)
            out.write(f"{name}\n")