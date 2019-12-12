import json

from pathlib import Path
from sqlalchemy import MetaData
from sqlalchemy.engine import Engine
from typing import Any, NamedTuple, List, Optional


class TableDescription(NamedTuple):
    name: str
    columns: List[str]
    values: List[List[Any]]

    def values_map(self, nullify_columns: Optional[List[str]] = None):
        nullify_columns = nullify_columns or []

        data = []
        for value in self.values:
            data.append({key: None if key in nullify_columns else value for key, value in zip(self.columns, value)})
        return data


def truncate(engine: Engine, meta: MetaData, exclude=None) -> None:
    for table in meta.tables.keys():
        if not exclude or table not in exclude:
            engine.execute(f'TRUNCATE table {table};')


def load_json_dump(path: Path) -> List[TableDescription]:
    json_dump = json.loads(path.read_text())

    tables: List[TableDescription] = []
    for table, values in json_dump.items():
        if not values:
            continue

        columns = list(values[0].keys())

        records = []
        for value in values:
            record = []
            for column in columns:
                column_value = value[column]

                if isinstance(column_value, dict):
                    column_value = json.dumps(column_value)

                if isinstance(column_value, str):
                    if column_value.isdigit():
                        column_value = int(column_value)
                    if column_value == 'null':
                        column_value = None

                record.append(column_value)
            records.append(record)
        tables.append(TableDescription(name=table, columns=columns, values=records))
    return tables


def apply_dump(engine: Engine, tables: List[TableDescription]) -> None:
    for table in tables:
        insert = f"INSERT INTO {table.name} ({','.join(table.columns)})" \
                 f" VALUES ({', '.join(('%s',) * len(table.columns))});"
        engine.execute(insert, table.values)
