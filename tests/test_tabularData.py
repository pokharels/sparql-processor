import pytest
import numpy as np
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
    def simple_three_join_query(self):
        query = """
        SELECT
            likes.subject,
            likes.object,
            hasReview.subject,
            hasReview.object,
            sentiment.subject,
            sentiment.object
        FROM sentiment, likes, hasReview
        WHERE likes.object = hasReview.subject
            AND hasReview.object = sentiment.subject
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

    @pytest.fixture
    def tabular_data_object(self):
        tab_data = TD(rdf_file_path="tests/test.txt")
        return tab_data

    @pytest.fixture
    def expected_join_result(self):
        join_result = []
        return join_result

    @pytest.fixture
    def tabular_three_join_data_object(self):
        tab_data = TD(rdf_file_path="tests/test_three_joins.txt")
        return tab_data

    @pytest.fixture
    def expected_three_join_result(self):
        result = {
            "likes.subject": ['User0', 'User0', 'User0', 'User1',
                              'User3', 'User0',],
            "likes.object": ['Product1', 'Product1', 'Product2', 'Product2',
                             'Product5', 'Product2'],
            "hasReview.subject": ['Product1', 'Product1', 'Product2',
                                  'Product2', 'Product5', 'Product2'],
            "hasReview.object": ['Review1', 'Review3', 'Review1', 'Review1',
                                 'Review9', 'Review1'],
            "sentiment.subject": ['Review1', 'Review3', 'Review1', 'Review1',
                                  'Review9', 'Review1'],
            "sentiment.object": ['S1', 'S0', 'S1', 'S1', 'S6', 'S1'],
        }

        return result

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

    def test_result_of_query_three_hash_join(
            self, simple_three_join_query,
            expected_three_join_result,
            tabular_three_join_data_object):

        result = tabular_three_join_data_object.execute_query(
            simple_three_join_query, "hash")

        for key, value in result.items():
            np.testing.assert_array_equal(
                value, expected_three_join_result[key])

    # def test_merge_sort_join(self, tabular_data_object, expected_join_result):
    #     assert type(tabular_data_object) is TD

    #     join_cols = ["likes.Object", 'hasReview.Subject']

    #     joined_data = tabular_data_object.join(table1='likes',
    #                                          table2='hasReview',
    #                                          on_columns=join_cols,
    #                                          join_type='merge_sort')

    #     assert type(joined_data) is TD
    #     np.testing.assert_array_equal(joined_data, expected_join_result)
