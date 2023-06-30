import argparse
import time
from src.DataLoader import DataLoader
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

    start_time = time.time()
    td = TabularData(args.data_path)
    td.execute_query(query, args.join_type)

    print(f"Query execution finished in {time.time() - start_time}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_path", default="./data/watdiv100k.txt")
    parser.add_argument(
        "--join_type", default="hash")

    args = parser.parse_args()

    main(args)
