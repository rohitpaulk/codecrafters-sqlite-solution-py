import sys
from typing import Optional, List, Tuple

from .models import (
    Index,
    Table,
    Record,
)

from .pages import (
    InteriorIndexBTreePage,
    InteriorTableBTreePage,
    LeafIndexBTreePage,
    LeafTableBTreePage,
    Page,
)

from .statement_parser import parse_statement

database_file_path = sys.argv[1]
command_or_statement = sys.argv[2]


SQLITE_SCHEMA_TABLE = Table(
    name="sqlite_schema",
    root_page=1,
    create_table_sql="CREATE TABLE sqlite_schema ( type text, name text, tbl_name text, rootpage text, sql text )",
    indexes=[]
)


def read_sqlite_schema_records(database_file_path):
    rows = read_table_rows(database_file_path, SQLITE_SCHEMA_TABLE)
    return [row for row in rows if row["tbl_name"] != b"sqlite_sequence"]


def read_sqlite_schema_table_row(database_file_path: str, table_name: str):
    for row in read_sqlite_schema_records(database_file_path):
        if row['type'] == b'table' and row['tbl_name'].decode('utf-8') == table_name:
            return row

    return None


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


def read_rows_using_index(database_file_path, table: Table, index: Index, filter_clauses: List[Tuple[str, str]]):
    def collect_rowids_from_interior_or_leaf_page(database_file, index: Index, page_number: int, value_to_filter_by: str):
        page = Page.parse_unknown_type_from(database_file, page_number)

        if page.is_leaf_index_btree_page:
            # print(f"Page#{page.number}: Hit leaf index btree page!")
            page = LeafIndexBTreePage.parse_from(database_file, page_number, index)

            # print(f"  - keys: {[cell.key for cell in page.cells]}")
            return [cell.rowid for cell in page.cells if cell.key == value_to_filter_by]
        elif page.is_interior_index_btree_page:
            # print(f"Page#{page.number}: Hit interior index btree page!")
            page = InteriorIndexBTreePage.parse_from(database_file, page_number, index)

            # print(f"  - pointers: {[(cell.key, cell.left_child_pointer) for cell in page.cells]}")

            row_ids = []

            for cell in page.cells:
                if cell.key == value_to_filter_by:
                    row_ids.append(cell.rowid)

                if cell.key >= value_to_filter_by:
                    row_ids += collect_rowids_from_interior_or_leaf_page(database_file, index, cell.left_child_pointer, value_to_filter_by)

                    if cell.key > value_to_filter_by:
                        break

            if page.cells[-1].key <= value_to_filter_by:
                row_ids += collect_rowids_from_interior_or_leaf_page(database_file, index, page.right_most_pointer, value_to_filter_by)

            return row_ids

    value_to_filter_by = filter_clauses[0][1]

    with open(database_file_path, "rb") as database_file:
        rowids = collect_rowids_from_interior_or_leaf_page(database_file, index, index.root_page, value_to_filter_by)
        return [Record(column_names_to_values={'id': rowid}) for rowid in rowids]


def handle_dot_command(command):
    if command == ".dbinfo":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(f"number of tables: {len(sqlite_schema_rows)}")
    elif command == ".tables":
        sqlite_schema_rows = read_sqlite_schema_records(database_file_path)
        print(" ".join([row['tbl_name'].decode('utf-8') for row in sqlite_schema_rows]))


def execute_statement(statement):
    parsed_statement = parse_statement(statement)

    table = get_table(database_file_path, parsed_statement.table_name)
    usable_index = table.find_index_for(parsed_statement.columns_used_in_filter_clauses)

    if usable_index:
        rows = read_rows_using_index(database_file_path, table, usable_index, parsed_statement.filter_clauses)
    else:
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