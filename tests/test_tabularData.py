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
            hasreview.subject,
            hasreview.object,
            sentiment.subject,
            sentiment.object
        FROM sentiment, likes, hasreview
        WHERE likes.object = hasreview.subject
            AND hasreview.object = sentiment.subject
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
            hasreview.object
        FROM follows, friendOf, likes, hasreview
        WHERE follows.object = friendOf.subject
            AND friendOf.object = likes.subject
            AND likes.object = hasreview.subject
        """
        return query

    @pytest.fixture
    def tabular_data_object_no_map(self):
        tab_data = TD(
            rdf_file_path="tests/test.txt", mapping=False)
        return tab_data

    @pytest.fixture
    def expected_join_result(self):
        join_result = []
        return join_result

    @pytest.fixture
    def tabular_three_join_data_object(self):
        tab_data = TD(rdf_file_path="tests/test_three_joins.txt",
                      mapping=False)
        return tab_data

    @pytest.fixture
    def expected_three_join_result(self):
        result = {
            "likes.subject": [
                'user0',
                'user0',
                'user1',
                'user10',
                'user0',
                'user3',
            ],
            "likes.object": [
                'product1',
                'product2',
                'product2',
                'product2',
                'product1',
                'product5',
            ],
            "hasreview.subject": [
                'product1',
                'product2',
                'product2',
                'product2',
                'product1',
                'product5',
            ],
            "hasreview.object": [
                'review1',
                'review1',
                'review1',
                'review1',
                'review3',
                'review9'
            ],
            "sentiment.subject": [
                'review1',
                'review1',
                'review1',
                'review1',
                'review3',
                'review9'
            ],
            "sentiment.object": ['s1', 's1', 's1', 's1', 's0', 's6'],
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

    def test_result_of_query_three_joins(
            self, simple_three_join_query,
            expected_three_join_result,
            tabular_three_join_data_object):

        # result = tabular_three_join_data_object.execute_query(
        #     simple_three_join_query, "hash")

        # assert result != {} and result is not None
        # for key, value in result.items():
        #     np.testing.assert_array_equal(
        #         value, expected_three_join_result[key])

        result = tabular_three_join_data_object.execute_query(
            simple_three_join_query, "merge_sort")
        assert result != {} and result is not None
        for key, value in result.items():
            np.testing.assert_array_equal(
                value, expected_three_join_result[key])

    # def test_projection_three_joins(self, tabular_three_join_data_object):
    #     expected_result = {
    #         "likes.subject": ['user0', 'user0', 'user0', 'user1',
    #                           'user3', 'user0',],
    #         "likes.object": ['product1', 'product1', 'product2', 'product2',
    #                          'product5', 'product2'],
    #         "hasreview.subject": ['product0', 'product1', 'product2',
    #                               'product2',  'product2'],
    #         "hasreview.object": ['review5', 'review3', 'review1', 'review1',
    #                              'review9'],
    #     }
    #     partial_join = {
    #         "likes.object": ['product1', 'product1', 'product2', 'product2',
    #                          'product5', 'product2'],
    #         "hasreview.subject": ['product0', 'product1', 'product2',
    #                               'product2', 'product2'],
    #     }
    #     indices_1 = [0, 1, 2, 3, 5]
    #     indices_2 = [1, 2, 3, 4]
    #     projected_result = tabular_three_join_data_object._projection(
    #         partial_join,
    #         "likes.object",
    #         indices_1,
    #         "hasreview.subject",
    #         indices_2
    #     )
    #     for key, value in projected_result.items():
    #         np.testing.assert_array_equal(
    #             value, expected_result[key])

    def test_join_3_joins(self, tabular_data_object_no_map):
        table_1 = [
            ('user0', 'product1', 'product1', "review3"),
            ('user0', 'product2', 'product2', 'review1'),
            ('user0', 'product2', 'product2', 'review9'),
            ('user1', 'product2', 'product2', 'review1'),
            ('user1', 'product2', 'product2', 'review9')
        ]
        table_2 = [
            ('review1', 's1'),
            ('review3', 's0'),
            ('review9', 's6')
        ]
        expected_result = [
            ["user0", "user1", "user0", "user0", "user1"],
            ["product2", "product2", "product1", "product2", "product2"],
            ["product2", "product2", "product1", "product2", "product2"],
            ["review1", "review1", "review3", "review9", "review9"],
            ["review1", "review1", "review3", "review9", "review9"],
            ["s1", "s1", "s0", "s6", "s6"]
        ]
        result = tabular_data_object_no_map._merge_sort_join(
            table_1, table_2)
        np.testing.assert_array_equal(expected_result, result)

        hash_result = tabular_data_object_no_map._hash_join(
            table_1, table_2)
        np.testing.assert_array_equal(expected_result, hash_result)

    def test_join_2_joins(self, tabular_data_object_no_map):
        table_1 = [
            ('user0', 'product1'),
            ('user0', 'product2'),
            ('user1', 'product2'),
            ('user3', 'product5')
        ]
        table_2 = [
            ('product0', 'review5'),
            ('product1', 'review3'),
            ('product2', 'review1'),
            ('product2', 'review9')
        ]
        expected_result = [
            ["user0", "user0", "user0", "user1", "user1"],
            ["product1", "product2", "product2", "product2", "product2"],
            ["product1", "product2", "product2", "product2", "product2"],
            ["review3", "review1", "review9", "review1", "review9"]
            # ('user0', 'product1', 'review3'),
            # ('user0', 'product2', 'review1'),
            # ('user0', 'product2', 'review9'),
            # ('user1', 'product2', 'review1'),
            # ('user1', 'product2', 'review1')
        ]
        result = tabular_data_object_no_map._merge_sort_join(
            table_1, table_2)
        np.testing.assert_array_equal(expected_result, result)

        hash_result = tabular_data_object_no_map._hash_join(
            table_1, table_2)
        np.testing.assert_array_equal(expected_result, hash_result)

    def test_join_three_joins_w_columns(self, tabular_three_join_data_object):
        cond1 = "likes.object"
        cond2 = "hasreview.subject"
        expected_result = {
            "likes.subject": [
                'user0',
                'user0',
                'user0',
                'user0',
                'user0',
                'user1',
                'user10',
                'user1',
                'user3',
            ],
            "likes.object": [
                'product1',
                'product1',
                'product1',
                'product1',
                'product2',
                'product2',
                'product2',
                'product3',
                'product5',
            ],
            "hasreview.subject": [
                'product1',
                'product1',
                'product1',
                'product1',
                'product2',
                'product2',
                'product2',
                'product3',
                'product5',
            ],
            "hasreview.object": [
                'review1',
                'review3',
                'review5',
                'review8',
                'review1',
                'review1',
                'review1',
                'review12',
                'review9'
            ],
        }

        result = tabular_three_join_data_object.join(
            cond1, cond2, partial={}, join_type="merge_sort")

        assert result != {} and result is not None
        for key, value in result.items():
            np.testing.assert_array_equal(
                sorted(value),
                sorted(expected_result[key])
            )

        hash_result = tabular_three_join_data_object.join(
            cond1, cond2, partial={}, join_type="hash")
        assert hash_result != {} and hash_result is not None
        for key, value in hash_result.items():
            np.testing.assert_array_equal(
                sorted(value),
                sorted(expected_result[key])
            )
