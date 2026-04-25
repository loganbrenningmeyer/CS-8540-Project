import os
from datasets import load_dataset, Dataset

out_path = "/home/ubuntu/data/clean_corpus/wikipedia_parquet"
os.makedirs(out_path, exist_ok=True)

ds = load_dataset(
    "wikimedia/wikipedia",
    "20231101.en",
    split="train",
    streaming=True,
)

stream = ds.shuffle(seed=42, buffer_size=100_000)

batch_size = 10_000
target_rows = 1_000_000

buffer = []
written = 0
part_id = 0

for sample in stream:
    buffer.append(sample)

    if len(buffer) >= batch_size:
        Dataset.from_list(buffer).to_parquet(
            f"{out_path}/part-{part_id:05d}.parquet"
        )

        written += len(buffer)
        part_id += 1
        buffer = []

        print(f"Written {written} rows")

        if written >= target_rows:
            break

if buffer and written < target_rows:
    Dataset.from_list(buffer).to_parquet(
        f"{out_path}/part-{part_id:05d}.parquet"
    )