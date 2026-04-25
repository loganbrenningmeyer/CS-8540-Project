from datasets import load_dataset


ds = load_dataset("wikimedia/wikipedia", "20231101.en", split="train")

print(f"{len(ds)=}")
print(ds.features)

total_chars = sum(len(x["text"]) for x in ds.select(range(10000)))
avg_chars = total_chars / 10000

estimated_total_chars = avg_chars * len(ds)

print("Avg chars:", avg_chars)
print("Estimated total chars:", estimated_total_chars)

# -------------------------
# Create subset of dataset Parquet table
# -------------------------
n_subset = 500_000
subset_ds = ds.shuffle(seed=42).select(range(n_subset)).select_columns(["id", "title", "text"])

subset_ds.to_parquet("./spark/data/wikipedia_parquet")