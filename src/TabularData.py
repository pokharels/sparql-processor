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
        return ""  # self.table

    def read_rdf(self, file_path: str, map_file: str = "mapping.json") -> dict:
        """
            Read File and parse data and store into tables, based on property
        """
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
            subject_value = ext_values[0].lower()
            property_key = ext_values[1].lower()
            object_value = ext_values[2].lower()

            # Add property to all_dicts if it doesn't exist
            if property_key not in all_dicts.keys():
                all_dicts[property_key] = {
                    'subject': [],
                    'object': []
                }

            # Add to mapper if it doesn't exist
            if subject_value not in mapper.keys():
                mapper[subject_value] = counter
                counter += 1

            # Add to all dicts
            all_dicts[property_key]['subject'].append(
                mapper[subject_value])

            # Repeat for object
            if object_value not in mapper.keys():
                mapper[object_value] = counter
                counter += 1

            all_dicts[property_key]['object'].append(
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
        # for table in tables:
        #     partial_join[table] = self.properties[table]

        # Process join_conditions: Perform the joins iteratively
        for cond in join_conditions:
            cond1, cond2 = cond.split("=")

            partial_join = self.join(
                partial_join,
                cond1.lower().strip(),
                cond2.lower().strip()
            )

            # table1, column1 = table_columns1[0].strip(), table_columns1[1].strip()
            # table2, column2 = table_columns2[0].strip(), table_columns2[1].strip()

            # # Perform the join
            # partial_join[f"{table1}_{table2}"] = self.join(
            #     partial_join[table1],
            #     partial_join[table2],
            #     on_columns={table1: column1, table2: column2},
            #     join_type=join_type
            # )
            # breakpoint()

        # Project the final result based on the SELECTed columns
        # result = {col: [] for col in columns}
        # for col in columns:
        #     table, column = col.split(".")
        #     result[col] = partial_join[table][column]

        return partial_join

    def _check_table_in_join_conditions(self):
        pass

    def _process_column_list(self, data):
        pass

    def join(self, partial_join, cond1, cond2, join_type: str = "hash"):
        (tab1, col1) = cond1.split(".")
        (tab2, col2) = cond2.split(".")

        if tab1 in partial_join:
            data1 = partial_join[cond1]
        else:
            data1 = self.properties[tab1][col1]

        if tab2 in partial_join:
            data2 = partial_join[cond2]
        else:
            data2 = self.properties[tab2][col2]

        if join_type == "hash":
            join_idx1, join_idx2 = self._hash_join(data1, data2)
        elif join_type == "merge_sort":
            join_idx1, join_idx2 = self._merge_sort_join(data1, data2)
        elif join_type == "radix_hash_join":
            join_idx1, join_idx2 = self._radix_hash_join(data1, data2)
        else:
            raise NotImplementedError("Join type not implemented")

        # partial_join[cond1] = join_indices
        # partial_join[cond2] = join_indices

        # Find projection and update partial join
        partial_join = self._projection(
            partial_join, cond1,  cond2, join_idx1, join_idx2)
        return partial_join

    def _projection(self, partial_join, cond1, col1_idx, cond2, col2_idx):
        # TODO: Implement projection as well.

        return partial_join

    def _hash_join(self, data1, data2):
        join_result = []
        return join_result

    def _merge_sort_join(self, data1, data2):
        """
            Assumption: table1 is dictionary with keys subject and object
            that contain lists
        """
        join_result = []
        i = 0
        return join_result, join_result

    def _radix_hash_join(self, data1, data2):
        join_result = []

        return join_result

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
