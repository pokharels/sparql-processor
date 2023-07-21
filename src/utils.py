"""
    Contains utils functionalities.
"""

import re
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


def read_watdiv_10M_dataset(file_path, mapping):
    """
        Data reader and processor specific to 10M dataset.
    """
    dataset_dict = {}
    mapper = {}
    counter = 0

    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            # Use regular expression to parse the triple
            match = re.match(
                r'<([^>]*)>\s+<([^>]*)>\s+("?[^"]*"?|\S+)', line.strip())
            if not match:
                continue

            subject, predicate, obj = match.groups()

            # Remove the < and > from the URLs
            subject = subject.strip()
            predicate = predicate.strip()
            obj = obj.strip('?"')

            # Use the predicate as the key
            key = predicate.split('/')[-1].split('#')[-1].lower()
            subject = subject.split('/')[-1].split('#')[-1].lower()
            obj = obj.split('/')[-1].split('#')[-1].split('>')[0].lower()

            if key not in dataset_dict:
                dataset_dict[key] = {
                    'subject': [],
                    'object': []
                }

            if mapping:
                # Add to mapper if it doesn't exist
                if subject not in mapper.keys():
                    mapper[subject] = counter
                    counter += 1
                # Add to all dicts
                dataset_dict[key]['subject'].append(
                    mapper[subject])

                # Repeat for object
                if obj not in mapper.keys():
                    mapper[obj] = counter
                    counter += 1

                dataset_dict[key]['object'].append(
                    mapper[obj])
            else:

                dataset_dict[key]['subject'].append(subject)
                dataset_dict[key]['object'].append(obj)

    return dataset_dict, mapper
