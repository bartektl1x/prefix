from dataclasses import dataclass

# Expected number of dots for different table name formats
DOTS_IN_SIMPLE_TABLE_NAME = 0          # e.g., "table"
DOTS_IN_FULL_TABLE_NAME = 2            # e.g., "catalog.schema.table"


@dataclass(frozen=True, slots=True)
class TableNameResolver:
    schema_prefix: str | None = None
    table_prefix: str | None = None

    def resolve_table_name(self, full_table_name: str) -> str:
        if not isinstance(full_table_name, str):
            full_table_name = str(full_table_name)

        dot_count = full_table_name.count(".")

        if dot_count not in (DOTS_IN_SIMPLE_TABLE_NAME, DOTS_IN_FULL_TABLE_NAME):
            exc = ValueError(
                "Table must be 'table' or 'catalog.schema.table'"
            )
            exc.add_note(f"received={full_table_name!r}")
            raise exc

        if not (self.schema_prefix or self.table_prefix):
            return full_table_name

        if dot_count == DOTS_IN_FULL_TABLE_NAME:
            catalog, schema, table = full_table_name.split(".")
            schema = self._apply(schema, self.schema_prefix)
            table = self._apply(table, self.table_prefix)
            return f"{catalog}.{schema}.{table}"

        return self._apply(full_table_name, self.table_prefix)

    @staticmethod
    def _apply(value: str, prefix: str | None) -> str:
        return f"{prefix}{value}" if prefix else value
