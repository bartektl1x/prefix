from dataclasses import dataclass


@dataclass(frozen=True)
class TableIdentifier:
    catalog: str
    schema: str
    table: str

    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @staticmethod
    def from_full_name(full_name: str) -> "TableIdentifier":
        normalized = full_name.strip()
        parts = normalized.split(".")

        if len(parts) != 3:
            exc = ValueError(
                "Fully qualified table name must have format 'catalog.schema.table'"
            )
            exc.add_note(f"Received value: {full_name}")
            raise exc

        catalog, schema, table = parts

        return TableIdentifier(
            catalog=catalog,
            schema=schema,
            table=table,
        )



from dataclasses import dataclass

from shared_lib.naming.table_identifier import TableIdentifier


@dataclass(frozen=True)
class TableNameResolver:
    schema_prefix: str | None = None
    table_prefix: str | None = None

    def resolve(self, identifier: TableIdentifier) -> TableIdentifier:
        schema = self._apply_prefix(identifier.schema, self.schema_prefix)
        table = self._apply_prefix(identifier.table, self.table_prefix)

        return TableIdentifier(
            catalog=identifier.catalog,
            schema=schema,
            table=table,
        )

    @staticmethod
    def _apply_prefix(name: str, prefix: str | None) -> str:
        if not prefix:
            return name

        return f"{prefix}{name}"






from abc import ABC
from pyspark.sql import SparkSession

from shared_lib.naming.table_name_resolver import TableNameResolver


class BasePipeline(ABC):

    def __init__(self, schemas_dir):
        self.spark = SparkSession.getActiveSession()
        self.schemas_dir = schemas_dir

        schema_prefix = self.spark.conf.get("pipeline.schema_prefix", None)
        table_prefix = self.spark.conf.get("pipeline.table_prefix", None)

        self.table_name_resolver = TableNameResolver(
            schema_prefix=schema_prefix,
            table_prefix=table_prefix,
        )





# example

from shared_lib.naming.table_identifier import TableIdentifier


identifier = TableIdentifier(
    catalog=config.target_table_catalog,
    schema=config.target_table_schema,
    table=config.target_table_name,
)

physical = self.table_name_resolver.resolve(identifier)


@dp.table(name=physical.full_name())
def table():
    return dataframe







# tests unit

from shared_lib.naming.table_identifier import TableIdentifier


class TestTableIdentifier:

    def test_full_name_representation(self):
        # given
        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        result = identifier.full_name()

        # then
        assert result == "analytics.silver.orders"






from shared_lib.naming.table_identifier import TableIdentifier
from shared_lib.naming.table_name_resolver import TableNameResolver


class TestTableNameResolver:

    def test_no_prefix_applied(self):
        # given
        resolver = TableNameResolver()

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.orders"

    def test_schema_prefix_applied(self):
        # given
        resolver = TableNameResolver(schema_prefix="dev_")

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.orders"

    def test_table_prefix_applied(self):
        # given
        resolver = TableNameResolver(table_prefix="tmp_")

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.tmp_orders"

    def test_schema_and_table_prefix_applied(self):
        # given
        resolver = TableNameResolver(
            schema_prefix="dev_",
            table_prefix="tmp_",
        )

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.tmp_orders"




# integration
import yaml
from pathlib import Path
from pyspark.sql import Row

from pipelines.base_pipeline import BasePipeline
from shared_lib.naming.table_identifier import TableIdentifier


class TestTablePrefixIntegration:

    def test_pipeline_resolves_prefixed_table_name(self, spark, tmp_path: Path):
        # given
        spark.conf.set("pipeline.schema_prefix", "dev_")
        spark.conf.set("pipeline.table_prefix", "tmp_")

        class TestPipeline(BasePipeline):
            pass

        pipeline = TestPipeline(tmp_path)

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = pipeline.table_name_resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.tmp_orders"
