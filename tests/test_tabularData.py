import pytest
from src.TabularData import TabularData as TD


class TestTabularData:

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

    def test_extract_from_sql_select(self, simple_query):
        _, columns, _ = TD._extract_from_sql_query(simple_query)
        expected_result = ["follows.subject", "follows.object"]
        assert columns == expected_result

    def test_extract_from_sql_from(self, simple_query):
        tables, _, _ = TD._extract_from_sql_query(simple_query)
        expected_result = "follows"
        assert tables == [expected_result]

    def test_extract_from_sql(self, simple_query):
        properties, columns, join_conditions = TD._extract_from_sql_query(
            simple_query)
        assert columns == ["follows.subject", "follows.object"]
        assert properties == ["follows"]
        assert join_conditions == [
            "follows.object = friendOf.subject"]
