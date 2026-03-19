
class TableNameResolver:
    def __init__(self, schema_prefix=None, table_prefix=None):
        self.schema_prefix = schema_prefix
        self.table_prefix = table_prefix

    def resolve_table_name(self, table: str, catalog: str, schema: str) -> str:
        dot_count = table.count(".")
        if dot_count not in (0, 2):
            raise ValueError("Table must be 'table' or 'catalog.schema.table'")

        if dot_count == 2:
            catalog, schema, table = table.split(".")

        schema = self._apply(schema, self.schema_prefix)
        table = self._apply(table, self.table_prefix)
        return f"{catalog}.{schema}.{table}"

    @staticmethod
    def _apply(value: str, prefix: str | None) -> str:
        return f"{prefix}{value}" if prefix else value

def resolve_table_name(
        self,
        table: str,
        catalog: str,
        schema: str,
    ) -> str:
        dot_count = table.count(".")

        if dot_count not in (0, 2):
            exc = ValueError("Table must be 'table' or 'catalog.schema.table'")
            exc.add_note(f"Received: {table}")
            raise exc

        if dot_count == 2:
            catalog, schema, table = table.split(".")

        schema = self._apply_prefix(schema, self._schema_prefix)
        table = self._apply_prefix(table, self._table_prefix)

        return f"{catalog}.{schema}.{table}"

    @staticmethod
    def _apply_prefix(value: str, prefix: str | None) -> str:
        if not prefix:
            return value
        return f"{prefix}{value}"
