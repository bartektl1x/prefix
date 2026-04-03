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

###########################



class TestPipelineConfig:

    def test_valid_config_with_static_catalog(self):
        # given
        config_dict = {
            "target_table_name": "my_table",
            "target_table_schema": "staging",
            "target_table_catalog": "gold_prod",
            "source_tables": [{"name": "table1", "alias": "t1"}],
            "custom_transformation": {
                "python_path": "transformations.books",
                "class_name": "FlattenBooks",
            },
        }

        # when
        config = PipelineConfig.model_validate(config_dict)

        # then
        assert config.target_table_catalog == "gold_prod"

    def test_valid_config_with_dynamic_catalog(self):
        # given
        config_dict = {
            "target_table_name": "my_table",
            "target_table_schema": "staging",
            "target_table_catalog": "{gold_catalog}",
            "source_tables": [{"name": "table1", "alias": "t1"}],
            "custom_transformation": {
                "python_path": "transformations.books",
                "class_name": "FlattenBooks",
            },
        }

        # when
        config = PipelineConfig.model_validate(config_dict)

        # then
        assert config.target_table_catalog == "{gold_catalog}"

    def test_invalid_catalog_placeholder_fails(self):
        # given
        config_dict = {
            "target_table_name": "my_table",
            "target_table_schema": "staging",
            "target_table_catalog": "{random_catalog}",
            "source_tables": [{"name": "table1", "alias": "t1"}],
            "custom_transformation": {
                "python_path": "transformations.books",
                "class_name": "FlattenBooks",
            },
        }

        # when / then
        with pytest.raises(ValueError):
            PipelineConfig.model_validate(config_dict)
