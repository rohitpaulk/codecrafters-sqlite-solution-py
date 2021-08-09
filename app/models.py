from dataclasses import dataclass
from typing import Dict, List
import sys


@dataclass
class Column:
    name: str
    type: str


@dataclass
class Table:
    name: str
    root_page: int
    create_table_sql: str

    @property
    def columns(self) -> List[Column]:
        tokens = self.create_table_sql.split()

        # Remove all the fluff
        tokens.pop(0)  # create
        tokens.pop(0)  # table
        tokens.pop(0)  # <table_name>
        tokens.pop(0)  # (
        tokens.pop()  # )

        column_definitions = " ".join(tokens).split(",")

        sys.stderr.write(self.create_table_sql)

        return [
            Column(
                name=column_definition.strip().split(" ", 1)[0],
                type=column_definition.strip().split(" ", 1)[1],  # Ignore constraints for now, assume this type
            )
            for column_definition in column_definitions
        ]


@dataclass
class Record:
    column_names_to_values: Dict[str, str]

    def __getitem__(self, item):
        return self.column_names_to_values[item]