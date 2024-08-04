# build_schema.py
# CLI script that generates DBT schema YML files

import json

from os import listdir, popen
from re import sub
from sys import argv

from duckdb import connect
from saneyaml import dump as yaml_dump
from saneyaml import load as yaml_load

WAREHOUSE_FILE = "./warehouse.duckdb"


def get_profile(
    schema: str,
    table: str,
    exclude_columns: None | list[str] = None,
) -> str:
    """
    get_profile

    Executes the print_profile_schema operation from the DBT Profiler
    package for a chosen warehouse model (needs to already be built);
    returns the YAML schema this operation builds as a string.

    Inputs:
        schema - schema containing the table/view
        table - name of the table/view to profile

    Optional:
        exclude_columns - do not profile these columns

    Output:
        profile_schema - YAML schema built by print_profile_schema

    """
    if not exclude_columns:
        cmd = f"""dbt run-operation print_profile_schema --args '{{"relation_name": "{table}", "schema": "{schema}"}}'"""  # noqa: E501
    else:
        exclusion_list = json.dumps(exclude_columns)
        cmd = f"""dbt run-operation print_profile_schema --args '{{"relation_name": "{table}", "schema": "{schema}", "exclude_columns": {exclusion_list}}}'"""  # noqa: E501

    profile_schema = ""

    start_recording = False  # we're going to ignore the first few lines of output

    print(f"Executing: {cmd}\n")

    with popen(cmd) as profiler_process:
        for output in profiler_process:
            if start_recording:
                profile_schema += output

            if "models:" in output:
                start_recording = True

    # return the schema for the target model:
    if profile_schema:
        return profile_schema

    # if no profile_schema was built, raise an exception:
    error_msg = f"No schema found in output of print_profile_schema for {schema}.{table}"
    raise ValueError(error_msg)


def get_column_values(
    field_name: str,
    table: str,
    schema: str,
) -> list:
    """
    get_column_values

    Returns a list of distinct values taken by a particular field
    in a particular relation in a particular dataset

    Inputs:
        field_name - name of the field to profile
        table - name of the parent table/view
        schema - location of the table/view within BigQuery

    Outputs:
        distinct_values - unique values of the field being profiled

    """
    conn = connect(WAREHOUSE_FILE)

    query = conn.execute(
        f"""
        SELECT DISTINCT {field_name} FROM {schema}.{table} WHERE {field_name} IS NOT null
        """,
    )

    rows = query.fetchall()

    distinct_values = sorted([record[0] for record in rows])

    return distinct_values


def get_accepted_vals_test(
    column_name: str,
    data_type: str,
    table: str,
    schema: str,
) -> dict[str, dict]:
    """
    get_accepted_vals_test

    Generates a test configuration for an accepted values DBT test
    to be applied to a specific data warehouse column

    Inputs:
        column_name - name of the categorical column
        data_type - the data type of the categorical column
        table - table or view hosting the column
        schema - schema hosting the table

    Outputs:
        test_configuration

    """
    # get the distinct values in the categorical column
    accepted_vals = get_column_values(column_name, table, schema)

    # DBT automatically wraps values in quotes, which is only appropriate for strings:
    if data_type == "string":
        test_definition = {"values": accepted_vals}
    else:
        test_definition = {"values": accepted_vals, "quote": False}  # type: ignore[dict-item]

    return {"accepted_values": test_definition}


def process_profile(  # noqa: C901
    profile: str,
    schema: str,
    table: str,
    excluded_columns: None | list = None,
) -> str:
    """
    process_profile

    Loads a YAML profile (provided as a string, assumed to be a
    single-element array at the top level. Operates on this schema,
    which should contain an array called 'columns', which have
    various properties in a dictionary called 'meta'.

    In particular, builds a new dictionary called 'test' if the contents
    of 'meta' suggests a not_null or unique test is appropriate; leaves
    the data type of the column in 'meta' and also indicates whether an
    'accepted values' test might be appropriate (could also indicate that
    the column should be a boolean)

    Inputs:
        profile - YAML profile
        schema - host schema of the table to be profiled
        table - name of the table/view to profile

    Optional:
        excluded_columns - columns that were not profiled

    Outputs:
        final_profile - YAML schema, post-processing

    """
    if excluded_columns is None:
        excluded_columns = []

    data = yaml_load(profile)[0]

    # preliminaries: add back in the unprofiled columns:
    for column_name, column_data_type, ordinal_position in excluded_columns:
        column_data = {
            "name": column_name,
            "description": "",
            "meta": {"data_type": column_data_type},
        }
        data["columns"].insert(ordinal_position, column_data)

    # main work: parse the data to build an appropriate schema:
    for column in data["columns"]:
        # next, add the tests:
        metadata = column["meta"]

        data_type = metadata["data_type"]

        if column["name"] in [c[0] for c in excluded_columns]:
            # in this case there is no profiling data and no way to add tests
            continue

        try:
            distinct_count = int(float(metadata["distinct_count"]))
        except KeyError as no_distinct_count:
            print(column)
            print(data_type)
            error_msg = "metadata didn't have distinct_count"
            raise KeyError(error_msg) from no_distinct_count

        is_unique = metadata["is_unique"]
        not_null = metadata["not_null_proportion"] == "1.0"
        all_null = metadata["not_null_proportion"] == "0.0"

        # avoid writing accepted_values tests when more than twenty distinct values:
        max_accepted_values_to_test = 20

        is_categorical = (
            data_type not in ["bool", "numeric"]
            and distinct_count <= max_accepted_values_to_test
            and not all_null
        )

        if is_unique or not_null or is_categorical:
            column["tests"] = []

            if not_null:
                column["tests"].append("not_null")

            if is_unique:
                column["tests"].append("unique")

            if is_categorical:
                test_definition = get_accepted_vals_test(
                    column["name"],
                    data_type,
                    table,
                    schema,
                )

                column["tests"].append(test_definition)

        # next, metadata:

        # remove most of the existing metadata ...
        del column["meta"]
        # ... add minimal metadata back in (this way it appears after the tests block)
        column["meta"] = {"data_type": data_type}
        if all_null:
            column["meta"]["all_null_warning"] = "!"

    return data


def compose_config_file(schema: str, file_location: str) -> str:

    tables = [t.split(".")[0] for t in listdir(file_location)]

    profiles = []

    for table in tables:

        profile = get_profile(schema, table)
        final_profile = process_profile(profile, schema, table)
        profiles.append(final_profile)

    nested_data = {"version": 2, "models": profiles}

    final_profile = yaml_dump(nested_data)

    # post-processing: fix some rendering issues:

    # add some spacing for readability
    final_profile = sub("\nmodels:\n", "\n\nmodels:\n", final_profile)
    final_profile = sub("\n  - name:", "\n\n\n  - name:", final_profile)
    # fix problem with description properties:
    final_profile = sub("description:\n", "description: ''\n", final_profile)
    # fix problem with quoted zeros:
    final_profile = sub("- '0'", "- 0", final_profile)
    # fix problem with empty strings:
    final_profile = sub("-\n", "- ''\n", final_profile)

    return final_profile


def write_output(complete_profile: str, file_location: str):
    """
    Writes out a DBT config file for a set of models in
    a specified location.

    The name of the file will match the location; the convention is
    /models/path/to/location gives a file like _path_to_location.yml

    Inputs:
        complete_profile - DBT config for the models
        file_location - directory to write the config file

    """
    config_name_parts = file_location.split("models/")[-1].split("/")

    joined_parts = "_".join(config_name_parts)

    file_path = f"{file_location}/_{joined_parts}.yml"

    try:
        with open(file_path, "x") as file_object:
            file_object.write(complete_profile)

    except FileExistsError:
        print(f"Config file {file_path} already exists, ignoring.")


if __name__ == "__main__":

    try:
        _, schema, file_location = argv
    except ValueError as incorrect_arguments:
        #  e.g. python build_schema.py dbt_mart models/mart
        emsg = "Proper call signature: python build_schema.py <schema> <file_location>"
        raise ValueError(emsg) from incorrect_arguments

    complete_profile = compose_config_file(schema, file_location)

    write_output(complete_profile, file_location)
