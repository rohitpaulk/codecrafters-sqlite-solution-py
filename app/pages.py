from dataclasses import dataclass
from typing import List

from app.models import PageHeader, DatabaseHeader, Record, Table, Index
from .varint_parser import parse_varint
from .record_parser import parse_index_record, parse_table_record


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
        elif page_header.is_interior_index_btree_page:
            return InteriorIndexBTreePage(header=page_header, number=page_number, size=page_size)
        elif page_header.is_leaf_index_btree_page:
            return LeafIndexBTreePage(header=page_header, number=page_number, size=page_size)

        raise Exception(f"Invalid page type: {page_header.page_type_description}")

    @property
    def cell_pointer_array_start_index(self):
        return self.start_index + self.file_header_size + self.header.size

    @property
    def file_header_size(self):
        return 100 if self.number == 1 else 0

    @property
    def is_leaf_index_btree_page(self):
        return self.header.is_leaf_index_btree_page

    @property
    def is_leaf_table_btree_page(self):
        return self.header.is_leaf_table_btree_page

    @property
    def is_interior_index_btree_page(self):
        return self.header.is_interior_index_btree_page

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
            self.records.append(parse_table_record(database_file, table, rowid))


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


@dataclass
class InteriorIndexBTreePageCell:
    left_child_pointer: int
    key: str
    rowid: int


class InteriorIndexBTreePage(Page):
    cells: List[InteriorIndexBTreePageCell]

    @classmethod
    def parse_from(cls, database_file, page_number, index: Index):
        page = Page.parse_unknown_type_from(database_file, page_number)
        page.parse_cells_from(database_file, index)
        return page

    def parse_cells_from(self, database_file, index: Index):
        cell_pointers = self.read_cell_pointers(database_file)

        self.cells = []

        for cell_pointer in cell_pointers:
            database_file.seek(self.start_index + cell_pointer)

            left_child_pointer = int.from_bytes(database_file.read(4), "big")
            number_of_bytes_in_payload = parse_varint(database_file)

            if cell_pointer + number_of_bytes_in_payload > self.size:
                raise Exception("Overflow page!")

            key, rowid = parse_index_record(database_file, index)
            self.cells.append(InteriorIndexBTreePageCell(left_child_pointer=left_child_pointer, key=key, rowid=rowid))

    @property
    def right_most_pointer(self):
        return self.header.right_most_pointer


@dataclass
class LeafIndexBTreePageCell:
    key: str
    rowid: int


class LeafIndexBTreePage(Page):
    cells: List[LeafIndexBTreePageCell]

    @classmethod
    def parse_from(cls, database_file, page_number, index: Index):
        page = Page.parse_unknown_type_from(database_file, page_number)
        page.parse_cells_from(database_file, index)
        return page

    def parse_cells_from(self, database_file, index: Index):
        cell_pointers = self.read_cell_pointers(database_file)

        self.cells = []

        for cell_pointer in cell_pointers:
            database_file.seek(self.start_index + cell_pointer)

            number_of_bytes_in_payload = parse_varint(database_file)

            if cell_pointer + number_of_bytes_in_payload > self.size:
                raise Exception("Overflow page!")

            key, rowid = parse_index_record(database_file, index)
            self.cells.append(LeafIndexBTreePageCell(key=key, rowid=rowid))

    @property
    def right_most_pointer(self):
        return self.header.right_most_pointer
