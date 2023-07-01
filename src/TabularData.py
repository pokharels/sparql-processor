"""
    Tabular Data Class.
"""

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
    def _extract_from_sql_query(query: str) -> tuple:
        query = " ".join(query.split())

        select_start = query.find("SELECT") + len("SELECT")
        select_end = query.find("FROM")
        select_clause = query[select_start:select_end].strip()

        from_start = select_end + len("FROM")
        from_end = query.find("WHERE")
        from_clause = query[from_start:from_end].strip()

        where_start = from_end + len("WHERE")
        where_clause = query[where_start:].strip()

        columns = [col.strip() for col in select_clause.split(",")]
        tables = [table.strip() for table in from_clause.split(",")]

        join_conditions = [
            condition.strip() for condition in where_clause.split("AND")]
        return tables, columns, join_conditions

    def execute_query(self, query: str, join_type: str):
        tables, columns, join_conditions = self._extract_from_sql_query(query)
        # TODO: Process tables, columns, join_conditions
        # Initialize the partial_join dictionary to store the intermediate results
        partial_join = {}

        # Process tables: Initialize the partial_join dictionary with table data
        for table in tables:
            partial_join[table] = self.properties[table]

        # Process join_conditions: Perform the joins iteratively
        for cond in join_conditions:
            cond1, cond2 = cond.split("=")
            table_columns1 = cond1.split(".")
            table_columns2 = cond2.split(".")
            table1, column1 = table_columns1[0].strip(), table_columns1[1].strip()
            table2, column2 = table_columns2[0].strip(), table_columns2[1].strip()

            # Perform the join
            partial_join[f"{table1}_{table2}"] = self.join(
                partial_join[table1],
                partial_join[table2],
                on_columns={table1: column1, table2: column2},
                join_type=join_type
            )
            breakpoint()

        # Project the final result based on the SELECTed columns
        result = {col: [] for col in columns}
        for col in columns:
            table, column = col.split(".")
            result[col] = partial_join[table][column]

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

    def nested_loop_join(table1, table2, on_columns):
        join_result = {col_name: [] for col_name in table1.keys()}

        # Get join column names and their respective table names
        join_column1 = on_columns[list(on_columns.keys())[0]]
        join_column2 = on_columns[list(on_columns.keys())[1]]

        # Find the index of the join columns in each table
        idx_join_col1 = table1[list(table1.keys())[0]].index(join_column1)
        idx_join_col2 = table2[list(table2.keys())[0]].index(join_column2)

        # Perform the nested loop join
        for row1 in zip(*table1.values()):
            for row2 in zip(*table2.values()):
                if row1[idx_join_col1] == row2[idx_join_col2]:
                    for col_name in table1.keys():
                        join_result[col_name].append(row1[table1[col_name].index(col_name)])
                    for col_name in table2.keys():
                        join_result[col_name].append(row2[table2[col_name].index(col_name)])

        return join_result

    def _hash_join(self, t1, t2):
        pass

    def _merge_sort_join(self, t1, t2):
        pass

    def _radix_hash_join(self, t1, t2):
        pass

    def _load_mapping_file(self, mapping_file_path: str) -> dict:
        pass

    def retrieve_string_data(self,
                             final_results,
                             file_path: str,
                             mapping_file_path: str):
        assert mapping_file_path is not None, "Mapping file found empty"
        mapping = self._load_mapping_file(mapping_file_path)
        # Swap key, value pair
        # return unmapped_data
        return mapping
