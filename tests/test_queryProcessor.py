import pytest
from src.QueryProcessor import QueryPreprocessor


class TestQueryPreprocessor:

    @pytest.fixture
    def simple_query(self):
        query = """
        SELECT follows.subject, follows.object
        FROM follows
        WHERE follows.object = friendOf.subject
        """
        return query

    @pytest.fixture
    def complex_query(self):
        query = """
        SELECT 
            follows.subject,
            follows.object,
            friendOf.object,
            likes.object,
            hasReview.object
        FROM follows, friendOf, likes, hasReview
        WHERE follows.object = friendOf.subject
            AND friendOf.object = likes.subject 
            AND likes.object = hasReview.subject
        """
        return query

    def test_get_clause(self, simple_query):

        pattern = r"SELECT\s+(.*?)\s+FROM"

        result = QueryPreprocessor._get_clause(simple_query, pattern)
        expected_result = "follows.subject, follows.object"
        assert result == expected_result

    def test_get_clause_where(self, simple_query):
        pattern = r"FROM\s+(.*?)\s+WHERE"

        result = QueryPreprocessor._get_clause(simple_query, pattern)
        expected_result = "follows"
        assert result == expected_result

    def test_get_clause_no_match(self, simple_query):
        pattern = r"WHERE\s+(.*?)$"

        with pytest.raises(AttributeError):
            QueryPreprocessor._get_clause(simple_query, pattern)

    def test_extract_from_sql(self, simple_query):
        preprocessor = QueryPreprocessor(simple_query)
        assert preprocessor.columns == ["follows.subject", "follows.object"]
        assert preprocessor.properties == ["follows"]
        assert preprocessor.join_conditions == [
            ("follows.object", "friendOf.subject")]
