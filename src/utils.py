"""
    Contains utils functionalities.
"""


import json


def read_file_lines(filepath: str) -> list:
    """
        Reads the given file line by line and returns list
    """
    with open(filepath, 'r', encoding="utf-8") as filereader:
        all_text = filereader.readlines()

    return all_text


def save_to_json(dicts: dict, filepath: str) -> None:
    """
        Saves the given dictionary to json file.
    """

    with open(filepath, "w", encoding="utf-8") as outfile:
        json.dump(dicts, outfile)
