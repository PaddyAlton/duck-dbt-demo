# warehouse/create.py
# simple script to set up a local data warehouse using DuckDB

import json
import os

from duckdb import connect

WAREHOUSE_FILE = "./warehouse.duckdb"


def create_warehouse(filename: str):
    """
    This function
    - creates a DuckDB file-based database
    - adds a single table, `products`
    - this table has an auto-incrementing `id` field
      and a VARCHAR `data` field.

    The file is overwritten if it already existed.

    Input:
        filename - name of the file where the database should be located

    """
    # functionality to *overwrite* file
    # so that this function restores it
    # to its initial state:
    if os.path.exists(filename):
        os.remove(filename)

    conn = connect(filename)

    conn.execute("CREATE SEQUENCE product_ids;")

    conn.execute("""
        CREATE TABLE products (
            id INTEGER DEFAULT NEXTVAL('product_ids'),
            data JSON
        );
    """)

    # we're going to insert some horribly denormalised data!
    denormed_data = [
        {"name": "gizmo", "versions": [1, 2, 3], "countries": ["GB", "US"]},
        {"name": "widget", "versions": [1, 2], "countries": ["DE", "FR", "IE"]},
    ]

    for product in denormed_data:
        conn.execute(
            "INSERT INTO products (data) VALUES(?);",
            [json.dumps(product)],
        )


def test_warehouse(filename: str) -> list:

    conn = connect(filename)

    results = conn.execute(
        "SELECT id, data->>'$.name' AS product_name FROM products;"
    ).fetchall()

    return results


if __name__ == "__main__":

    create_warehouse(WAREHOUSE_FILE)

    test_results = test_warehouse(WAREHOUSE_FILE)

    print(test_results)
