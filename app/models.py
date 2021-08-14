from dataclasses import dataclass
from typing import Dict, List, Optional, Any


@dataclass
class Column:
    name: str
    type: str
    is_primary_key: bool


@dataclass
class Index:
    name: str
    root_page: int
    create_index_sql: str

    @property
    def column_names(self):
        sql = self.create_index_sql.strip().split("(")[1]
        sql = sql.split(")")[0]

        return [sql]


@dataclass
class Table:
    name: str
    root_page: int
    create_table_sql: str
    indexes: List[Index]

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

    def find_index_for(self, column_names) -> Optional[Index]:
        for column_name in column_names:
            for index in self.indexes:
                if column_name in index.column_names:
                    return index

        return None


@dataclass
class Record:
    column_names_to_values: Dict[str, Any]
    rowid: int

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

        if not (instance.is_leaf_page ^ instance.is_interior_page):
            raise Exception(f"expected a leaf or interior page, got: {instance.page_type_description}")

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
        return self.is_leaf_table_btree_page or self.is_leaf_index_btree_page

    @property
    def is_interior_page(self):
        return self.is_interior_index_btree_page or self.is_interior_table_btree_page

    @property
    def is_leaf_index_btree_page(self):
        return self.page_type == 10

    @property
    def is_leaf_table_btree_page(self):
        return self.page_type == 13

    @property
    def is_interior_index_btree_page(self):
        return self.page_type == 2

    @property
    def is_interior_table_btree_page(self):
        return self.page_type == 5

    @property
    def page_type_description(self):
        return {
            2: 'interior index btree',
            5: 'interior table btree',
            10: 'leaf index btree',
            13: 'leaf table btree',
        }[self.page_type]