import argparse
from src.DataLoader import DataLoader
from src.QueryProcessor import QueryPreprocessor
from src.TabularData import TabularData


def main(args):
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

    d_loader = DataLoader(args.data_path)
    td = TabularData([])
    td.execute_query(query)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_path", default="./data/watdiv100k.txt")
    args = parser.parse_args()

    main(args)
