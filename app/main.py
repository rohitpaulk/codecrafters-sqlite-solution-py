import sys
from typing import Optional
from dataclasses import dataclass

import sqlparse

from .models import Table, Record
from .record_parser import parse_record
from .varint_parser import parse_varint


@dataclass(init=False)
class PageHeader:
    page_type: int
    first_free_block_start: int
    number_of_cells: int
    start_of_content_area: int
    fragmented_free_bytes: int
    right_most_pointer: int  # Only applicable for interior pages

    @classmethod
    def parse_from(cls, database_file):
        instance = cls()

        instance.page_type = int.from_bytes(database_file.read(1), "big")

        if not (instance.is_leaf_page ^ instance.is_interior_page):
            raise Exception(f"expected a leaf or interior page, got: {instance.page_type}")

        instance.first_free_block_start = int.from_bytes(database_file.read(2), "big")
        instance.number_of_cells = int.from_bytes(database_file.read(2), "big")
        instance.start_of_content_area = int.from_bytes(database_file.read(2), "big")
        instance.fragmented_free_bytes = int.from_bytes(database_file.read(1), "big")
        instance.right_most_pointer = int.from_bytes(database_file.read(4), "big") if instance.is_interior_page else None

        return instance

    @property
    def size(self):
        return 8 if self.is_leaf_page else 12

    @property
    def is_leaf_page(self):
        return self.page_type == 13

    @property
    def is_interior_page(self):
        return self.page_type == 5


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


def read_records_from_table_leaf_page(database_file_path, table, page_number):
    file_header_size = 100 if page_number == 1 else 0

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = (page_number - 1) * page_size
        database_file.seek(page_start + file_header_size)

        page_header = PageHeader.parse_from(database_file)

        database_file.seek(page_start + file_header_size + page_header.size)
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

        records = []

        # Each of these cells represents a row in the sqlite_schema table.
        for index, cell_pointer in enumerate(cell_pointers):
            database_file.seek(page_start + cell_pointer)

            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)  # Use this!
            records.append(parse_record(database_file, table))

        return records


def read_nodes_from_table_interior_page(database_file_path, table, page_number):
    file_header_size = 100 if page_number == 1 else 0  # Should always be 0 for an interior page?

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = (page_number - 1) * page_size
        database_file.seek(page_start + file_header_size)

        page_header = PageHeader.parse_from(database_file)
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

        nodes = []

        # Each of these cells represents a row in the sqlite_schema table.
        for index, cell_pointer in enumerate(cell_pointers):
            database_file.seek(page_start + cell_pointer)

            left_child_page = int.from_bytes(database_file.read(4), "big")
            rowid = parse_varint(database_file)  # Use this!
            nodes.append({'id': rowid, 'left_child_page': left_child_page})

        return nodes


def read_table_rows(database_file_path: str, table: Table):
    file_header_size = 100 if table.root_page == 1 else 0

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = (table.root_page - 1) * page_size
        database_file.seek(page_start + file_header_size)

        page_type = int.from_bytes(database_file.read(1), "big")
        print(f"page_type: {page_type}")

        is_interior_page = (page_type == 5)
        is_leaf_page = (page_type == 13)

        if not (is_leaf_page ^ is_interior_page):
            raise Exception(f"expected either a leaf page of interior page, got: {page_type}")

        if is_leaf_page:
            records = read_records_from_table_leaf_page(database_file_path, table, table.root_page)
        else:
            btree_nodes = read_nodes_from_table_interior_page(database_file_path, table, table.root_page)
            for btree_node in btree_nodes:
                print(btree_node)

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
        aggregations = []
        columns_to_select = []
        filter_clauses = []

        current_index, token_after_select = parsed_statement.token_next(0)

        if isinstance(token_after_select, sqlparse.sql.Function):
            function_name_token = token_after_select.token_matching(lambda token: isinstance(token, sqlparse.sql.Identifier), 0)
            function_name = str(function_name_token)

            if function_name.lower() == "count":
                aggregations.append("COUNT")
            else:
                raise Exception(f"Unknown function: {function_name}")
        else:
            column_name_tokens = token_after_select if isinstance(token_after_select, sqlparse.sql.Identifier) else token_after_select.get_identifiers()
            columns_to_select = [str(column_name_token) for column_name_token in column_name_tokens]

        current_index, _from_token = parsed_statement.token_next(current_index)
        current_index, table_name_token = parsed_statement.token_next(current_index)
        table_name = str(table_name_token)

        current_index, token_after_table_name = parsed_statement.token_next(current_index)

        if isinstance(token_after_table_name, sqlparse.sql.Where):
            where_token_list = token_after_table_name
            comparison_token = where_token_list.token_matching(lambda token: isinstance(token, sqlparse.sql.Comparison), 0)
            filter_clauses.append((str(comparison_token.left), str(comparison_token.right).strip("'")))

        table = get_table(database_file_path, table_name)
        print(table.columns)
        rows = read_table_rows(database_file_path, table)

        for filter_clause in filter_clauses:
            rows = [row for row in rows if row[filter_clause[0]].decode('utf-8') == filter_clause[1]]

        if aggregations:
            print(len(rows))
        else:
            for row in rows:
                print("|".join(row[column_name].decode('utf-8') for column_name in columns_to_select))
    else:
        raise Exception(f"Unknown SQL statement: {statement}")


if command_or_statement.startswith("."):
    handle_dot_command(command_or_statement)
else:
    execute_statement(command_or_statement)