import json
from typing import Any, Iterable, TextIO


def print_nested_keys(data: dict, depth: int = 0):
    for key in data:
        print("\t"*depth + f"{key}: {type(data[key])}")
        if type(data[key]) is dict:
            print_nested_keys(data[key], depth=depth + 1)


def gather_hashtags(data: dict) -> list:
    if len(data["entities"]["hashtags"]) > 0:
        return [
            item["text"]
            for item in data["entities"]["hashtags"]
        ]
    return []


def gather_urls(data: dict) -> list:
    if len(data["entities"]["urls"]) > 0:
        return [
            item["expanded_url"]
            for item in data["entities"]["urls"]
        ]
    return []


def write_items_to_txt(items: list[str], out_path: str):
    with open(out_path, "w") as f:
        for item in items:
            f.write(item + "\n")


def main():
    json_path = "./out.json"

    hashtags = []
    urls = []

    with open(json_path, "r") as f:
        i = 1
        for line in f.readlines():
            data = json.loads(line.strip())
            hashtags.extend(gather_hashtags(data))
            urls.extend(gather_urls(data))

    write_items_to_txt(hashtags, "./hashtags.txt")
    write_items_to_txt(urls, "./urls.txt")
            


if __name__ == "__main__":
    main()