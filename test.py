from pathlib import Path

from pyspark.sql import SparkSession

from shared_lib.naming.table_identifier import TableIdentifier
from custom_layer.pipeline import CustomPipeline


class TestBasePipelineTableNameIntegration:

    def test_prefix_applied_from_spark_conf(self, spark: SparkSession, tmp_path: Path):
        # given
        spark.conf.set("schema_prefix", "dev_")
        spark.conf.set("table_prefix", "tmp_")

        pipeline = CustomPipeline(tmp_path)

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = pipeline.table_name_resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.tmp_orders"

    def test_no_prefix_returns_original_name(self, spark: SparkSession, tmp_path: Path):
        # given
        spark.conf.set("schema_prefix", "")
        spark.conf.set("table_prefix", "")

        pipeline = CustomPipeline(tmp_path)

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = pipeline.table_name_resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.orders"

    def test_partial_prefix_schema_only(self, spark: SparkSession, tmp_path: Path):
        # given
        spark.conf.set("schema_prefix", "dev_")
        spark.conf.set("table_prefix", "")

        pipeline = CustomPipeline(tmp_path)

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = pipeline.table_name_resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.dev_silver.orders"

    def test_partial_prefix_table_only(self, spark: SparkSession, tmp_path: Path):
        # given
        spark.conf.set("schema_prefix", "")
        spark.conf.set("table_prefix", "tmp_")

        pipeline = CustomPipeline(tmp_path)

        identifier = TableIdentifier(
            catalog="analytics",
            schema="silver",
            table="orders",
        )

        # when
        resolved = pipeline.table_name_resolver.resolve(identifier)

        # then
        assert resolved.full_name() == "analytics.silver.tmp_orders"
