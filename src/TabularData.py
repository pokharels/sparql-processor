import re


class TabularData:
    def __init__(self, data: list):
        self.data = self._process_column_list(data)

    def __repr__(self) -> list:
        """ String representation of a TabularData object
        [
        property1:
            {
                subject: [val1, val2, val3, ... ,valn],
                object: [val1, val2, val3, ... ,valn]
            },
        property2:
            {
                subject: [val1, val2, val3, ... ,valn],
                object: [val1, val2, val3, ... ,valn]
            }
        ]
        :return: Tabular Data string representation
        :rtype: str
        """
        return self.table

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

    def execute_query(self, query: str):
        tables, columns, join_conditions = self._extract_from_sql_query(query)
        # TODO: Process tables, columns, join_conditions
        result = []
        return result

    def _process_column_list(self, data):
        return ""

    def join(self, other_td, on_columns: list, type: str = "hash"):
        pass

    def _hash_join(self):
        pass

    def _merge_sort_join(self):
        pass

    def _radix_hash_join(self):
        pass
