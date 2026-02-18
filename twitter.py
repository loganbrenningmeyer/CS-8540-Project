import json

TWITTER_FILE_PATH = "out.json"

def json_get_fields_recursive(json_obj, result):
    if isinstance(json_obj, dict):
        for key, value in json_obj.items():
            key_lower = key.lower()

            # Only grab the value if it's a non-empty string
            if "url" in key_lower:
                if isinstance(value, str) and value.strip():
                    result.setdefault("extracted_urls", []).append(value)

            # Get the 'text' field only when inside a hashtag list
            elif key_lower == "hashtags" and isinstance(value, list):
                tags = [tag.get("text") for tag in value if isinstance(tag, dict) and tag.get("text")]
                result.setdefault("extracted_hashtags", []).extend(tags)
                # Dont need to recurse into hashtags
                continue

            # Continue recursing for nested structures
            json_get_fields_recursive(value, result)
    
    # If it's a list, we need to check each item in the list
    elif isinstance(json_obj, list):
        for item in json_obj:
            json_get_fields_recursive(item, result)

def extract_data(json_obj):
    result = {}
    json_get_fields_recursive(json_obj, result)
    return result


def get_data():
    data = []
    try:
        with open(TWITTER_FILE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    tweet_json = json.loads(line)
                    data.append(extract_data(tweet_json))
        return data
            
    except FileNotFoundError:
        print(f"Error: {TWITTER_FILE_PATH} not found.")
        return []


def merge_data(data_list):
    merged_data = {}
    for data in data_list:
        for key, value in data.items():
            if key not in merged_data:
                merged_data[key] = set()
            merged_data[key].update(value)
    
    # Convert sets back to lists
    for key in merged_data:
        merged_data[key] = list(merged_data[key])
    
    return merged_data


def save_merged_data(merged_data, output_file):
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, indent=2)

def main():

    try:
        data = get_data()
        if data:
            print(f"Extraction result for tweet n:\n{json.dumps(data[33], indent=2)}")
            
    except FileNotFoundError:
        print(f"Error: {TWITTER_FILE_PATH} not found.")


    merge_data_result = merge_data(data)

    save_merged_data(merge_data_result, "merged_output.json")



if __name__ == "__main__":
    main()