from dataclasses import dataclass
from typing import Dict, List
import sys

from .varint_parser import parse_varint


@dataclass
class Column:
    name: str
    type: str
    is_primary_key: bool


@dataclass
class Table:
    name: str
    root_page: int
    create_table_sql: str

    @property
    def columns(self) -> List[Column]:
        sql = self.create_table_sql.strip().split("(")[1]
        sql = sql.split(")")[0]

        tokens = [x.strip() for x in sql.split()]
        column_definitions = [x.strip() for x in " ".join(tokens).split(",")]

        return [
            Column(
                name=column_definition.strip().split(" ", 1)[0],
                type=column_definition.strip().split(" ", 1)[1],  # Ignore constraints for now, assume this type
                is_primary_key='primary key' in column_definition.lower(),
            )
            for column_definition in column_definitions
        ]


@dataclass
class Record:
    column_names_to_values: Dict[str, str]

    def __getitem__(self, item):
        return self.column_names_to_values[item]


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
        from .record_parser import parse_record  # Avoid circular import

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