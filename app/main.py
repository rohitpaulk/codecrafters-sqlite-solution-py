import sys
from typing import Optional

from .models import (
    Table,
    Page,
    LeafTableBTreePage,
    Index,
    InteriorTableBTreePage
)

from .statement_parser import parse_statement

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
    def collect_records_from_interior_or_leaf_page(database_file, page_number):
        page = Page.parse_unknown_type_from(database_file, page_number)

        if page.is_leaf_table_btree_page:
            page = LeafTableBTreePage.parse_from(database_file, page_number, table)
            return page.records
        elif page.is_interior_table_btree_page:
            page = InteriorTableBTreePage.parse_from(database_file, page_number, table)

            records = []

            for cell in page.cells:
                records += collect_records_from_interior_or_leaf_page(database_file, cell.left_child_pointer)

            records += collect_records_from_interior_or_leaf_page(database_file, page.right_most_pointer)

            return records

    with open(database_file_path, "rb") as database_file:
        return collect_records_from_interior_or_leaf_page(database_file, table.root_page)


def handle_dot_command(command):
    if command == ".dbinfo":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(f"number of tables: {len(sqlite_schema_rows)}")
    elif command == ".tables":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(" ".join([row['tbl_name'].decode('utf-8') for row in sqlite_schema_rows]))


def read_sqlite_schema_table_row(database_file_path: str, table_name: str):
    for row in read_sqlite_schema_records(database_file_path):
        if row['type'] == b'table' and row['tbl_name'].decode('utf-8') == table_name:
            return row


def read_sqlite_schema_index_rows(database_file_path: str, table_name: str):
    return [
        row for row in read_sqlite_schema_records(database_file_path)
        if row['type'] == b'index' and row['tbl_name'].decode('utf-8') == table_name
    ]


def get_table(database_file_path :str, table_name: str) -> Optional[Table]:
    sqlite_schema_table_row = read_sqlite_schema_table_row(database_file_path, table_name)
    sqlite_schema_index_rows = read_sqlite_schema_index_rows(database_file_path, table_name)

    return Table(
        name=sqlite_schema_table_row['tbl_name'].decode('utf-8'),
        root_page=sqlite_schema_table_row['rootpage'],
        create_table_sql=sqlite_schema_table_row['sql'].decode('utf-8'),
        indexes=[
            Index(
                name=sqlite_schema_index_row['name'].decode('utf-8'),
                root_page=sqlite_schema_index_row['rootpage'],
                create_index_sql=sqlite_schema_index_row['sql'].decode('utf-8')
            )
            for sqlite_schema_index_row in sqlite_schema_index_rows
        ]
    )


def execute_statement(statement):
    parsed_statement = parse_statement(statement)

    table = get_table(database_file_path, parsed_statement.table_name)
    rows = read_table_rows(database_file_path, table)

    for filter_clause in parsed_statement.filter_clauses:
        rows = [row for row in rows if (row[filter_clause[0]] or b"").decode('utf-8') == filter_clause[1]]

    if parsed_statement.aggregations:
        print(len(rows))
    else:
        def format_value(value):
            if value is None:
                return ""

            if isinstance(value, int):
                return str(value)

            return value.decode('utf-8')

        for row in rows:
            print("|".join(format_value(row[column_name]) for column_name in parsed_statement.columns_to_select))


if command_or_statement.startswith("."):
    handle_dot_command(command_or_statement)
else:
    execute_statement(command_or_statement)