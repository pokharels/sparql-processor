class TabularData:
    def __init__(self, data: list):
        self.table = self._process_column_list(data)

    def __repr__(self) -> list:
        return self.table

    def _process_column_list(self, data):
        return ""

    def join(self, other_td, on_columns: list, type: str = "hash"):
        pass

    def hash_join(self):
        pass

    def merge_sort_join(self):
        pass

    def radix_hash_join(self):
        pass
