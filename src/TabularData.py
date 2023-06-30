"""
    Tabular Data Class.
"""

import re
from src.TabularDataFrame import TabularDataHolder
from src.utils import read_file_lines, save_to_json


class TabularData:
    def __init__(self, rdf_file_path: str):
        self.properties = self.read_rdf(rdf_file_path)

    def __repr__(self) -> dict:
        """ String representation of a TabularData object
        {
        property1:
            {
                TabularDataFrame
                subject_column: [val1, val2, val3, ... ,valn],
                object_column: [val1, val2, val3, ... ,valn]
            },
        property2:
            {
                TabularDataFrame
                subject_column: [val1, val2, val3, ... ,valn],
                object_column: [val1, val2, val3, ... ,valn]
            }
        }
        :return: Tabular Data string representation
        :rtype: str
        """
        return self.table

    def read_rdf(self, file_path: str, map_file: str = "mapping.json") -> dict:
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
                all_dicts[property_key] = {
                    'Subject': [],
                    'Object': []
            }

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
        save_to_json(mapper, map_file)

        print(f'***Total unmatched lines: {unmatched_counter} out \
                of {len(all_lines)}***')

        return all_dicts

    @staticmethod
    def _get_clause(query: str, pattern: str) -> re.Match:
        return re.search(pattern, query, re.IGNORECASE).group(1)

    @staticmethod
    def _extract_from_sql_query(query: str) -> tuple:
        select_pattern = r"SELECT\s+(.*?)\s+FROM"
        from_pattern = r"FROM\s+(.*?)\s+WHERE"
        where_pattern = r"WHERE\s+(.*)"

        select_clause = TabularData._get_clause(query, select_pattern)
        from_clause = TabularData._get_clause(query, from_pattern)
        where_clause = TabularData._get_clause(query, where_pattern)

        columns = [col.strip() for col in select_clause.split(',')]
        tables = [table.strip() for table in from_clause.split(',')]

        join_conditions = []
        where_conditions = where_clause.split('AND')
        for condition in where_conditions:
            condition = condition.strip()
            left, right = condition.split('=')
            join_conditions.append((left.strip(), right.strip()))

        return tables, columns, join_conditions

    def execute_query(self, query: str, join_type: str):
        tables, columns, join_conditions = self._extract_from_sql_query(query)
        # TODO: Process tables, columns, join_conditions
        # for t1_name, t2_name in zip(tables, tables[1:]):
        #     table1 = self.properties[t1_name]
        #     table2 = self.properties[t2_name]

        #     partial_join = self.join(table1, table2, on_columns=, join_type=join_type)
        result = []
        # table0 = self.properties[tables[0]]
        return result

    def _check_table_in_join_conditions(self):
        pass

    def _process_column_list(self, data):
        pass

    def join(self, table1, table2, on_columns: list, join_type: str = "hash"):
        if join_type == "hash":
            self._hash_join(table1, table2)
        elif join_type == "merge_sort":
            self._merge_sort_join(table1, table2)
        elif join_type == "radix_hash_join":
            self._radix_hash_join(table1, table2)
        else:
            raise NotImplementedError

    def _hash_join(self, t1, t2):
        pass

    def _merge_sort_join(self, t1, t2):
        pass

    def _radix_hash_join(self, t1, t2):
        pass
