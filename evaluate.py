import argparse
import time
from src.TabularData import TabularData
from src.utils import save_to_json


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
    td = TabularData(args.data_path, mapping=args.mapping)

    result = td.execute_query(query, args.join_type)
    print(f"Query execution finished in {time.time() - start_time}s")
    save_to_json(result, args.result_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_path", default="./data/watdiv100k.txt")
    parser.add_argument(
        "--join_type", default="hash")
    parser.add_argument(
        "--result_path", default="./results/results.json")
    parser.add_argument(
        "--mapping", type=bool, default=True)

    args = parser.parse_args()

    main(args)
