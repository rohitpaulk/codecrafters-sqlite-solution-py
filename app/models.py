from dataclasses import dataclass
from typing import Dict, List
import sys


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