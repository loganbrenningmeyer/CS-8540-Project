import json
from pathlib import Path
from tqdm import tqdm


def write_json_to_txt(json_path: str, out_dir: str):
    """
    """
    # -------------------------
    # Read JSON data to dict
    # -------------------------
    with open(json_path, "r") as f:
        data = json.load(f)

    # -------------------------
    # Write JSON values to .txt files
    # -------------------------
    for key, values in data.items():
        with open(Path(json_path).parent / f"{key}.txt", "w") as f:
            for value in values:
                f.write(f"{value}\n")


def main():
    json_path = "./merged_output.json"
    out_dir = "./data"

    write_json_to_txt(json_path, out_dir)


if __name__ == "__main__":
    main()