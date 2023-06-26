import re


class QueryPreprocessor:
    def __init__(self, query: str):
        (self.properties,
            self.columns,
            self.join_conditions
         ) = self._extract_from_sql_query(query)

    @staticmethod
    def _get_clause(query: str, pattern: str) -> re.Match:
        return re.search(pattern, query, re.IGNORECASE).group(1)

    def _extract_from_sql_query(self, query: str) -> None:
        select_pattern = r"SELECT\s+(.*?)\s+FROM"
        from_pattern = r"FROM\s+(.*?)\s+WHERE"
        where_pattern = r"WHERE\s+(.*)"

        select_clause = self._get_clause(query, select_pattern)
        from_clause = self._get_clause(query, from_pattern)
        where_clause = self._get_clause(query, where_pattern)

        columns = [col.strip() for col in select_clause.split(',')]
        tables = [table.strip() for table in from_clause.split(',')]

        join_conditions = []
        where_conditions = where_clause.split('AND')
        for condition in where_conditions:
            condition = condition.strip()
            left, right = condition.split('=')
            join_conditions.append((left.strip(), right.strip()))

        return tables, columns, join_conditions
