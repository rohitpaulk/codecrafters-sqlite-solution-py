import sys
from typing import Optional

import sqlparse

from .models import Table, Record
from .record_parser import parse_record
from .varint_parser import parse_varint

database_file_path = sys.argv[1]
command_or_statement = sys.argv[2]


SQLITE_SCHEMA_TABLE = Table(
    name="sqlite_schema",
    root_page=1,
    create_table_sql="CREATE TABLE sqlite_schema ( type text, name text, tbl_name text, rootpage text, sql text )"
)


def read_sqlite_schema_records(database_file_path):
    rows = read_table_rows(database_file_path, SQLITE_SCHEMA_TABLE)
    return [row for row in rows if row["tbl_name"] != b"sqlite_sequence"]


def read_table_rows(database_file_path: str, table: Table):
    page_header_size = 100 if table.root_page == 1 else 0

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = (table.root_page - 1) * page_size
        database_file.seek(page_start + page_header_size)

        _page_type = int.from_bytes(database_file.read(1), "big")
        _first_freeblock_start = int.from_bytes(database_file.read(2), "big")
        number_of_cells = int.from_bytes(database_file.read(2), "big")

        database_file.seek(page_start + page_header_size + 8)
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(number_of_cells)]

        records = []

        # Each of these cells represents a row in the sqlite_schema table.
        for cell_pointer in cell_pointers:
            database_file.seek(page_start + cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            records.append(parse_record(database_file, table))

        return records


def handle_dot_command(command):
    if command == ".dbinfo":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(f"number of tables: {len(sqlite_schema_rows)}")
    elif command == ".tables":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(" ".join([row['tbl_name'].decode('utf-8') for row in sqlite_schema_rows]))


def read_sqlite_schema_row(database_file_path: str, table_name: str):
    for row in read_sqlite_schema_records(database_file_path):
        if row['tbl_name'].decode('utf-8') == table_name:
            return row


def get_table(database_file_path :str, table_name: str) -> Optional[Table]:
    sqlite_schema_row = read_sqlite_schema_row(database_file_path, table_name)

    return Table(
        name=sqlite_schema_row['tbl_name'].decode('utf-8'),
        root_page=sqlite_schema_row['rootpage'],
        create_table_sql=sqlite_schema_row['sql'].decode('utf-8')
    )


def execute_statement(statement):
    parsed_statement = sqlparse.parse(statement)[0]

    if parsed_statement.get_type() == "SELECT":
        current_index, token_after_select = parsed_statement.token_next(0)

        if isinstance(token_after_select, sqlparse.sql.Function):
            function_name_token = token_after_select.token_matching(lambda token: isinstance(token, sqlparse.sql.Identifier), 0)
            function_name = str(function_name_token)

            if function_name.lower() == "count":
                table_name_token = parsed_statement.token_matching(lambda token: isinstance(token, sqlparse.sql.Identifier), current_index)
                table = get_table(database_file_path, str(table_name_token))
                rows = read_table_rows(database_file_path, table)
                print(len(rows))
            else:
                raise Exception(f"Unknown function: {function_name}")
        else:
            column_name_tokens = token_after_select if isinstance(token_after_select, sqlparse.sql.Identifier) else token_after_select.get_identifiers()
            current_index, _from_token = parsed_statement.token_next(current_index)
            current_index, table_name_token = parsed_statement.token_next(current_index)

            column_names = [str(column_name_token) for column_name_token in column_name_tokens]
            table_name = str(table_name_token)

            table = get_table(database_file_path, table_name)
            rows = read_table_rows(database_file_path, table)

            for row in rows:
                print("|".join(row[column_name].decode('utf-8') for column_name in column_names))
    else:
        raise Exception(f"Unknown SQL statement: {statement}")


if command_or_statement.startswith("."):
    handle_dot_command(command_or_statement)
else:
    execute_statement(command_or_statement)