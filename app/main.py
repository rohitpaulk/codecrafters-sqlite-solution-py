import sys

from dataclasses import dataclass
from typing import Optional, List, Dict

from .record_parser import parse_record
from .varint_parser import parse_varint

database_file_path = sys.argv[1]
command_or_statement = sys.argv[2]


@dataclass
class Table:
    name: str
    root_page: int
    create_table_sql: str

    @property
    def columns(self):
        tokens = self.create_table_sql.split()

        # Remove all the fluff
        tokens.pop(0)  # create
        tokens.pop(0)  # table
        tokens.pop(0)  # <table_name>
        tokens.pop(0)  # (
        tokens.pop()  # )

        column_definitions = " ".join(tokens).split(", ")

        return [
            Column(
                name=column_definition.split(" ", 1)[0],
                type=column_definition.split(" ", 1)[1],  # Ignore constraints for now, assume this type
            )
            for column_definition in column_definitions
        ]


SQLITE_SCHEMA_TABLE = Table(
    name="sqlite_schema",
    root_page=1,
    create_table_sql="CREATE TABLE (type text, name text, tbl_name text, rootpage text, sql text)"
)


@dataclass
class Column:
    name: str
    type: str


@dataclass
class Record:
    column_names_to_values: Dict[str, str]

    def value_for(self, column_name):
        return self.column_names_to_values[column_name]


def read_sqlite_schema_rows(database_file_path):
    with open(database_file_path, "rb") as database_file:
        database_file.seek(100)  # Skip the header section

        _page_type = int.from_bytes(database_file.read(1), "big")
        _first_freeblock_start = int.from_bytes(database_file.read(2), "big")
        number_of_cells = int.from_bytes(database_file.read(2), "big")

        database_file.seek(100 + 8)  # Skip the database header & b-tree page header, get to the cell pointer array

        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(number_of_cells)]

        sqlite_schema_rows = []

        # Each of these cells represents a row in the sqlite_schema table.
        for cell_pointer in cell_pointers:
            database_file.seek(cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            record = parse_record(database_file, 5)

            # Table contains columns: type, name, tbl_name, rootpage, sql
            sqlite_schema_rows.append({
                'type': record[0],
                'name': record[1],
                'tbl_name': record[2],
                'rootpage': record[3],
                'sql': record[4],
            })

        return [row for row in sqlite_schema_rows if row['tbl_name'] != b"sqlite_sequence"]


def read_table_rows(database_file_path: str, rootpage: int):
    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)  # Header string
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = (rootpage - 1) * page_size
        database_file.seek(page_start)

        _page_type = int.from_bytes(database_file.read(1), "big")
        _first_freeblock_start = int.from_bytes(database_file.read(2), "big")
        number_of_cells = int.from_bytes(database_file.read(2), "big")

        database_file.seek(page_start + 8)
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(number_of_cells)]

        # for cell_pointer in cell_pointers:
        #     database_file.seek(page_start + cell_pointer)
        #     _number_of_bytes_in_payload = parse_varint(database_file)
        #     rowid = parse_varint(database_file)
        #     record = parse_record(database_file, 5)
        #
        #     # Table contains columns: type, name, tbl_name, rootpage, sql
        #     sqlite_schema_rows.append({
        #         'type': record[0],
        #         'name': record[1],
        #         'tbl_name': record[2],
        #         'rootpage': record[3],
        #         'sql': record[4],
        #     })

        return cell_pointers


def handle_dot_command(command):
    if command == ".dbinfo":
        sqlite_schema_rows = read_sqlite_schema_rows(database_file_path)
        print(f"number of tables: {len(sqlite_schema_rows)}")
    elif command == ".tables":
        sqlite_schema_rows = read_sqlite_schema_rows(database_file_path)
        print(" ".join([row['tbl_name'].decode('utf-8') for row in sqlite_schema_rows]))


def read_sqlite_schema_row(database_file_path: str, table_name: str):
    for row in read_sqlite_schema_rows(database_file_path):
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
    if statement.startswith("select count(*)"):
        table_name = statement.split(" ")[-1]
        sqlite_schema_row = read_sqlite_schema_row(database_file_path, table_name)
        rows = read_table_rows(database_file_path, sqlite_schema_row['rootpage'])
        print(len(rows))
    elif statement.startswith("select"):
        column_name = statement.split(" ")[1]
        table_name = statement.split(" ")[-1]
        table = get_table(database_file_path, table_name)
        print(column_name, f"table: {table_name}", 'columns: ', list(map(lambda x: [x.name, x.type], table.columns)))
    else:
        raise Exception(f"Unknown SQL statement: {statement}")


if command_or_statement.startswith("."):
    handle_dot_command(command_or_statement)
else:
    execute_statement(command_or_statement)