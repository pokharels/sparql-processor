"""
    Data Loader class.
"""

from src.TabularData import TabularData
from src.utils import read_file_lines, save_to_json


class DataLoader:
    """
        Data Loader class, reads the raw data, finds and saves mapping
        and returns lists of dictionary for all properties.
    """

    def __init__(self, file_path: str) -> None:
        self.processed_data = self._parse_rdf_data(file_path)

    def _parse_rdf_data(self, file_path: str) -> dict:
        # Read File and parse data and store into tables, based on property
        all_lines = read_file_lines(filepath=file_path)

        mapper = {}
        counter = 0
        unmatched_counter = 0

        all_dicts = {}

        for line in all_lines:
            line_split = line.replace('\t', ' ').replace(' .\n', '').split(" ")

            # IndexError occurs where the ":" pattern does not match
            try:
                ext_values = [x.split(":")[1] for x in line_split]

            except IndexError:
                unmatched_counter += 1
                continue

            # Swap the first and second element so that property
            # always comes first.
            subject_value = ext_values[0]
            property_key = ext_values[1]
            object_value = ext_values[2]

            # Add property to all_dicts if it doesn't exist
            if property_key not in all_dicts.keys():
                all_dicts[property_key] = {'Subject': [],
                                           'Object': []}

            # Add to mapper if it doesn't exist
            if subject_value not in mapper.keys():
                mapper[subject_value] = counter
                counter += 1

            # Add to all dicts
            all_dicts[property_key]['Subject'].append(
                mapper[subject_value])

            # Repeat for object
            if object_value not in mapper.keys():
                mapper[object_value] = counter
                counter += 1
            
            all_dicts[property_key]['Object'].append(
                mapper[object_value])
            
        # Save mapping file
        save_to_json(mapper, "mapper.json")

        print(f'***Total unmatched lines: {unmatched_counter} out \
                of {len(all_lines)}***')

        return all_dicts

    # def _map_string_to_integers(self, counter):
    #     # Map into integer
    #     pass

    def _load_mapping_file(self, mapping_file_path: str) -> dict:
        pass

    def retrieve_string_data(self,
                             final_results: TabularData,
                             file_path: str,
                             mapping_file_path: str) -> TabularData:
        assert mapping_file_path is not None, "Mapping file found empty"
        mapping = self._load_mapping_file(mapping_file_path)
        # Swap key, value pair
        # return unmapped_data
        return mapping
