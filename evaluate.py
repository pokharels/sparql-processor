import argparse
from src.DataLoader import DataLoader
from src.QueryProcessor import QueryPreprocessor
from src.TabularData import TabularData


def main(args):
    d = DataLoader(args.data_path)
    q = QueryPreprocessor()
    td = TabularData([])
    print("HELLO WORLD", d, q, td)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data_path", default="./data/watdiv100k.txt")
    args = parser.parse_args()

    main(args)
