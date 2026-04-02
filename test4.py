class TestSourceTableConfig:

    def test_valid_name_without_placeholder(self):
        # given
        source_dict = {"name": "schema.table", "alias": "t"}

        # when
        source = SourceTableConfig.model_validate(source_dict)

        # then
        assert source.model_dump() == {
            "name": "schema.table",
            "alias": "t",
            "read_mode": "batch",
        }

    def test_valid_name_with_bronze_catalog_placeholder(self):
        # given
        source_dict = {"name": "{bronze_catalog}.schema.table", "alias": "t"}

        # when
        source = SourceTableConfig.model_validate(source_dict)

        # then
        assert source.model_dump() == {
            "name": "{bronze_catalog}.schema.table",
            "alias": "t",
            "read_mode": "batch",
        }

    def test_invalid_placeholder_fails(self):
        # given
        source_dict = {"name": "{random_catalog}.schema.table", "alias": "t"}

        # when / then
        with pytest.raises(ValueError):
            SourceTableConfig.model_validate(source_dict)
