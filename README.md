SPARQL Processor
---

Run SPARQL query with SQL syntax.
Currenlty, only RDF triplets with the simplest SQL queries are implemented with `SELECT`, `FROM`, `WHERE` clauses.

### Interacting with TabularData object

```
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

td = TabularData("./rdf_data.txt")
result = td.execute_query(query, join_type="hash")
```

### Running program

Run the `evaluate.py` to execute the default query over the `watdiv100k` datasets.
The dataset path as well as the join type can be changed as arguments to the scripts.

```
usage: evaluate.py [-h] [--data_path {./data/watdiv100k.txt,./data/watdiv.10M.nt}] [--join_type {hash,merge_sort,improved_hash_join}] [--result_path RESULT_PATH] [--mapping MAPPING]
```