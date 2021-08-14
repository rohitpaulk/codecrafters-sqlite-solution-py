from dataclasses import dataclass
from typing import List, Tuple

import sqlparse


@dataclass
class SelectQuery:
    aggregations: List[str]
    columns_to_select: List[str]
    filter_clauses: List[Tuple[str, str]]
    table_name: str

    @property
    def columns_used_in_filter_clauses(self):
        return [filter_clause[0] for filter_clause in self.filter_clauses]


def parse_statement(statement):
    sqlparse_statement = sqlparse.parse(statement)[0]

    if sqlparse_statement.get_type() == "SELECT":
        aggregations = []
        columns_to_select = []
        filter_clauses = []

        current_index, token_after_select = sqlparse_statement.token_next(0)

        if isinstance(token_after_select, sqlparse.sql.Function):
            function_name_token = token_after_select.token_matching(
                lambda token: isinstance(token, sqlparse.sql.Identifier), 0)
            function_name = str(function_name_token)

            if function_name.lower() == "count":
                aggregations.append("COUNT")
            else:
                raise Exception(f"Unknown function: {function_name}")
        else:
            column_name_tokens = token_after_select if isinstance(token_after_select,
                                                                  sqlparse.sql.Identifier) else token_after_select.get_identifiers()
            columns_to_select = [str(column_name_token) for column_name_token in column_name_tokens]

        current_index, _from_token = sqlparse_statement.token_next(current_index)
        current_index, table_name_token = sqlparse_statement.token_next(current_index)
        table_name = str(table_name_token)

        current_index, token_after_table_name = sqlparse_statement.token_next(current_index)

        if isinstance(token_after_table_name, sqlparse.sql.Where):
            where_token_list = token_after_table_name
            comparison_token = where_token_list.token_matching(lambda token: isinstance(token, sqlparse.sql.Comparison), 0)
            filter_clauses.append((str(comparison_token.left), str(comparison_token.right).strip("'")))

        return SelectQuery(
            aggregations=aggregations,
            columns_to_select=columns_to_select,
            filter_clauses=filter_clauses,
            table_name=table_name
        )
    else:
        raise Exception(f"Unknown SQL statement: {statement}")
