# File Organization

## Inputs

Input files for word count are at `input/` which contains `hashtags.txt` and `urls.txt`, text files which contain each of the hashtags and urls parsed from the twitter data with a line for each instance.

## Outputs

Output word counts and logs are at `output/` within `output/hashtags/` and `output/urls/` for the hashtag and url outputs, respectively.

Within the hashtag and url output directories, there are folders for `.../hadoop/` and `.../spark`.

- `.../hadoop/`

  - Within the hadoop outputs, there is a `part-r-00000` file containing the word count results, and a `wordcount_<...>_run.log` file, containing the hadoop run log.

- `.../spark/`

  - Within the spark outputs, there is a single `spare_<...>_output.txt` file containing both the word count results and spark logs.


