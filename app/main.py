import sys
from typing import Optional, List
from dataclasses import dataclass
import time

import sqlparse

from .models import Table, Record
from .record_parser import parse_record
from .varint_parser import parse_varint


@dataclass
class DatabaseHeader:
    page_size: int

    @classmethod
    def parse_from(cls, database_file) -> "DatabaseHeader":
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")
        return cls(page_size=page_size)


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

        if not (instance.is_leaf_table_btree_page ^ instance.is_interior_table_btree_page):
            raise Exception(f"expected a leaf or interior page, got: {instance.page_type_description}")

        instance.first_free_block_start = int.from_bytes(database_file.read(2), "big")
        instance.number_of_cells = int.from_bytes(database_file.read(2), "big")
        instance.start_of_content_area = int.from_bytes(database_file.read(2), "big")
        instance.fragmented_free_bytes = int.from_bytes(database_file.read(1), "big")
        instance.right_most_pointer = int.from_bytes(database_file.read(4), "big") if instance.is_interior_table_btree_page else None

        return instance

    @property
    def size(self):
        return 8 if self.is_leaf_page else 12

    @property
    def is_leaf_page(self):
        return self.is_leaf_table_btree_page # TODO: Include index pages too

    @property
    def is_leaf_table_btree_page(self):
        return self.page_type == 13

    @property
    def is_interior_table_btree_page(self):
        return self.page_type == 5

    @property
    def page_type_description(self):
        return {
            5: 'interior table btree',
            13: 'leaf table btree'
        }[self.page_type]


@dataclass
class Page:
    header: PageHeader
    number: int
    size: int

    @classmethod
    def parse_unknown_type_from(cls, database_file, page_number):
        file_header_size = 100 if page_number == 1 else 0

        page_size = DatabaseHeader.parse_from(database_file).page_size
        page_start = (page_number - 1) * page_size

        database_file.seek(page_start + file_header_size)

        page_header = PageHeader.parse_from(database_file)

        if page_header.is_leaf_table_btree_page:
            return LeafTableBTreePage(header=page_header, number=page_number, size=page_size)
        elif page_header.is_interior_table_btree_page:
            return InteriorTableBTreePage(header=page_header, number=page_number, size=page_size)

        raise Exception(f"Invalid page type: {page_header.page_type_description}")

    @property
    def cell_pointer_array_start_index(self):
        return self.start_index + self.file_header_size + self.header.size

    @property
    def file_header_size(self):
        return 100 if self.number == 1 else 0

    @property
    def is_leaf_table_btree_page(self):
        return self.header.is_leaf_table_btree_page

    @property
    def is_interior_table_btree_page(self):
        return self.header.is_interior_table_btree_page

    @property
    def start_index(self):
        return (self.number - 1) * self.size

    def read_cell_pointers(self, database_file):
        database_file.seek(self.cell_pointer_array_start_index)
        return [int.from_bytes(database_file.read(2), "big") for _ in range(self.header.number_of_cells)]


class LeafTableBTreePage(Page):
    records: List[Record]

    @classmethod
    def parse_from(cls, database_file, page_number, table: Table):
        page = Page.parse_unknown_type_from(database_file, page_number)
        page.parse_records_from(database_file, table)

        return page

    def parse_records_from(self, database_file, table: Table):
        cell_pointers = self.read_cell_pointers(database_file)

        self.records = []

        # Each of these cells represents a row in the sqlite_schema table.
        for index, cell_pointer in enumerate(cell_pointers):
            database_file.seek(self.start_index + cell_pointer)

            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            self.records.append(parse_record(database_file, table, rowid))


@dataclass
class InteriorTableBTreePageCell:
    left_child_pointer: int
    key: int


class InteriorTableBTreePage(Page):
    cells: List[InteriorTableBTreePageCell]

    @classmethod
    def parse_from(cls, database_file, page_number, _table: Table):
        page = Page.parse_unknown_type_from(database_file, page_number)
        page.parse_cells_from(database_file)
        return page

    def parse_cells_from(self, database_file):
        cell_pointers = self.read_cell_pointers(database_file)

        self.cells = []

        for index, cell_pointer in enumerate(cell_pointers):
            database_file.seek(self.start_index + cell_pointer)

            left_child_pointer = int.from_bytes(database_file.read(4), "big")
            key = parse_varint(database_file)
            self.cells.append(InteriorTableBTreePageCell(left_child_pointer=left_child_pointer, key=key))

    @property
    def right_most_pointer(self):
        return self.header.right_most_pointer


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
        rows = read_table_rows(database_file_path, table)

        for filter_clause in filter_clauses:
            rows = [row for row in rows if (row[filter_clause[0]] or b"").decode('utf-8') == filter_clause[1]]

        if aggregations:
            print(len(rows))
        else:
            def format_value(value):
                if value is None:
                    return ""

                if isinstance(value, int):
                    return str(value)

                return value.decode('utf-8')

            for row in rows:
                print("|".join(format_value(row[column_name]) for column_name in columns_to_select))
    else:
        raise Exception(f"Unknown SQL statement: {statement}")


if command_or_statement.startswith("."):
    handle_dot_command(command_or_statement)
else:
    execute_statement(command_or_statement)