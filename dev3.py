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
