from src.TabularData import TabularData


class DataLoader:
    def __init__(self, file_path: str) -> None:
        self.tables = self._parse_rdf_data(file_path)

    def _parse_rdf_data(self, file_path: str) -> list[dict]:
        # Read File and parse data and store into tables, based on property
        data = None
        mapped_data = self._map_string_to_integers(data)
        # Save mapped data
        # Save mapping file
        return mapped_data

    def _map_string_to_integers(self, string_set: set):
        # Map into integer
        pass

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
