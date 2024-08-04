# duck-dbt-demo

This is a demonstration project for the pairing of DuckDB with DBT.

This combination should be good for prototyping.
- DuckDB (a hyper-flexible column-store database) can be used as a kind of 'local data warehouse'
- DBT is a popular data transformation tool for data warehousing purposes

## Trying it out

The name of the game is 'keep it lightweight'. For this project I'm managing everything via `poetry`; there's no need for containerisation since even the 'warehouse' is just a file.

If you run `poetry install` you will end up with `duckdb` and `dbt` installed in a local virtual environment.

You can then run DBT commands prefixed with `poetry run`, e.g. `poetry run dbt run`.

## Other notes

I've thrown in a few 'extras':
- `warehouse/create.py` will reset DuckDB so that the DBT models are removed, recreating the source tables in their original state
- there's a custom `generate_schema_name` macro that puts DBT models into different schemas within DuckDB
- `tools/build_schema.py` is a script that can build DBT config files from materialised models
