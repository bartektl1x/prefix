from dataclasses import dataclass


@dataclass(frozen=True)
class TableNameResolver:
    schema_prefix: str | None = None
    table_prefix: str | None = None

    def resolve_identifier(self, identifier: TableIdentifier) -> TableIdentifier:
        return TableIdentifier(
            catalog=identifier.catalog,
            schema=self._apply_prefix(identifier.schema, self.schema_prefix),
            table=self._apply_prefix(identifier.table, self.table_prefix),
        )

    def resolve_full_name(self, full_name: str) -> TableIdentifier:
        identifier = TableIdentifier.from_full_name(full_name)
        return self.resolve_identifier(identifier)

    @staticmethod
    def _apply_prefix(name: str, prefix: str | None) -> str:
        if not prefix:
            return name
        return f"{prefix}{name}"




import pytest

from shared_lib.naming.table_identifier import TableIdentifier
from shared_lib.naming.table_name_resolver import TableNameResolver


class TestTableIdentifier:

    def test_from_full_name_valid(self):
        # given
        full_name = "catalog.schema.table"

        # when
        identifier = TableIdentifier.from_full_name(full_name)

        # then
        assert identifier.catalog == "catalog"
        assert identifier.schema == "schema"
        assert identifier.table == "table"

    def test_from_full_name_invalid_format(self):
        # given
        full_name = "invalid_name"

        # when / then
        with pytest.raises(ValueError) as exc:
            TableIdentifier.from_full_name(full_name)

        assert "catalog.schema.table" in str(exc.value)


class TestTableNameResolver:

    def test_resolve_identifier_without_prefix(self):
        # given
        resolver = TableNameResolver()
        identifier = TableIdentifier("catalog", "schema", "table")

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "catalog.schema.table"

    def test_resolve_identifier_with_prefix(self):
        # given
        resolver = TableNameResolver(
            schema_prefix="dev_",
            table_prefix="tmp_",
        )
        identifier = TableIdentifier("catalog", "schema", "table")

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "catalog.dev_schema.tmp_table"

    def test_resolve_full_name(self):
        # given
        resolver = TableNameResolver(
            schema_prefix="dev_",
            table_prefix="tmp_",
        )

        # when
        resolved = resolver.resolve_full_name("catalog.schema.table")

        # then
        assert resolved.full_name() == "catalog.dev_schema.tmp_table"

    def test_resolve_full_name_invalid(self):
        # given
        resolver = TableNameResolver()

        # when / then
        with pytest.raises(ValueError):
            resolver.resolve_full_name("invalid")






import pytest
from pyspark.sql import SparkSession

from shared_lib.naming.table_identifier import TableIdentifier
from shared_lib.naming.table_name_resolver import TableNameResolver


class TestTableNameResolverIntegration:

    def test_prefix_applied_from_spark_conf_with_parts(self, spark: SparkSession):
        # given
        spark.conf.set("schema_prefix", "dev_")
        spark.conf.set("table_prefix", "tmp_")

        resolver = TableNameResolver(
            schema_prefix=spark.conf.get("schema_prefix"),
            table_prefix=spark.conf.get("table_prefix"),
        )

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.tmp_orders"

    def test_prefix_applied_from_spark_conf_with_full_name(self, spark: SparkSession):
        # given
        spark.conf.set("schema_prefix", "dev_")
        spark.conf.set("table_prefix", "tmp_")

        resolver = TableNameResolver(
            schema_prefix=spark.conf.get("schema_prefix"),
            table_prefix=spark.conf.get("table_prefix"),
        )

        # when
        resolved = resolver.resolve_full_name("analytics.silver.orders")

        # then
        assert resolved.full_name() == "analytics.dev_silver.tmp_orders"

    def test_no_prefix_keeps_original_name(self, spark: SparkSession):
        # given
        spark.conf.set("schema_prefix", "")
        spark.conf.set("table_prefix", "")

        resolver = TableNameResolver(
            schema_prefix=spark.conf.get("schema_prefix"),
            table_prefix=spark.conf.get("table_prefix"),
        )

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.orders"

    def test_partial_prefix_only_schema(self, spark: SparkSession):
        # given
        spark.conf.set("schema_prefix", "dev_")
        spark.conf.set("table_prefix", None)

        resolver = TableNameResolver(
            schema_prefix=spark.conf.get("schema_prefix"),
            table_prefix=spark.conf.get("table_prefix"),
        )

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.orders"

    def test_partial_prefix_only_table(self, spark: SparkSession):
        # given
        spark.conf.set("schema_prefix", None)
        spark.conf.set("table_prefix", "tmp_")

        resolver = TableNameResolver(
            schema_prefix=spark.conf.get("schema_prefix"),
            table_prefix=spark.conf.get("table_prefix"),
        )

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve_identifier(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.tmp_orders"
