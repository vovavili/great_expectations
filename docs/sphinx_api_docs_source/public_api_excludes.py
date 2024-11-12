"""Methods, classes and files to be excluded from consideration as part of the public API.

Include here methods that share a name with another method for example (since we use string matching
to determine what is used in our documentation code snippets).
"""

from __future__ import annotations

import pathlib

from docs.sphinx_api_docs_source.include_exclude_definition import (
    IncludeExcludeDefinition,
)

DEFAULT_EXCLUDES: list[IncludeExcludeDefinition] = [
    IncludeExcludeDefinition(
        reason="We now use get_context(), this method only exists for backward compatibility.",
        name="DataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="We now use get_context(), this method only exists for backward compatibility.",
        name="BaseDataContext",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/base_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Fluent is not part of the public API",
        filepath=pathlib.Path("great_expectations/datasource/fluent/interfaces.py"),
    ),
    IncludeExcludeDefinition(
        reason="Fluent is not part of the public API",
        name="read_csv",
        filepath=pathlib.Path("great_expectations/datasource/fluent/config.py"),
    ),
    IncludeExcludeDefinition(
        reason="Fluent-style read_csv is not referenced in the docs yet, but due to string matching it is being flagged.",
        name="read_csv",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/pandas_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Marshmallow dump methods are not part of the public API",
        name="dump",
        filepath=pathlib.Path("great_expectations/data_context/types/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from __init__.py",
        filepath=pathlib.Path("great_expectations/types/__init__.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/databricks_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/glob_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/manual_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/query_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/s3_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/s3_subdir_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/subdir_reader_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        filepath=pathlib.Path(
            "great_expectations/datasource/batch_kwargs_generator/table_batch_kwargs_generator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="ValidationActions are now run from Checkpoints: https://docs.greatexpectations.io/docs/guides/miscellaneous/migration_guide#manually-migrate-v2-checkpoints-to-v3-checkpoints",
        name="run",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="CLI internal methods should not be part of the public API",
        filepath=pathlib.Path("great_expectations/cli/datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="CLI internal methods should not be part of the public API",
        filepath=pathlib.Path("great_expectations/cli/toolkit.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for from datasource_configuration_test_utilities import is_subset",
        name="is_subset",
        filepath=pathlib.Path("great_expectations/core/domain.py"),
    ),
    IncludeExcludeDefinition(
        reason="Already captured in the Data Context",
        name="test_yaml_config",
        filepath=pathlib.Path(
            "great_expectations/data_context/config_validator/yaml_config_validator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.get_metric()",
        name="get_metric",
        filepath=pathlib.Path(
            "great_expectations/core/expectation_validation_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.suites.get()",
        name="get_expectation_suite",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.save_expectation_suite() and validator.save_expectation_suite()",
        name="save_expectation_suite",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.validate()",
        name="validate",
        filepath=pathlib.Path("great_expectations/data_asset/data_asset.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for validator.validate()",
        name="validate",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/batch_filter.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="add_checkpoint",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="create_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="get_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="list_checkpoints",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="list_expectation_suite_names",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="save_expectation_suite",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/cloud_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Captured in AbstractDataContext",
        name="add_store",
        filepath=pathlib.Path(
            "great_expectations/data_context/data_context/file_data_context.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/_store_backend.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/_store_backend.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/datasource_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path("great_expectations/data_context/store/query_store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path("great_expectations/data_context/store/query_store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for python dict `.get()`",
        name="get",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for python `set()`",
        name="set",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.add_checkpoint()",
        name="add_checkpoint",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/checkpoint_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.list_checkpoints()",
        name="list_checkpoints",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/checkpoint_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/configuration_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/html_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/json_site_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path("great_expectations/data_context/store/store.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for datasource self_check",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/validation_results_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for yaml.dump()",
        name="dump",
        filepath=pathlib.Path("great_expectations/data_context/templates.py"),
    ),
    IncludeExcludeDefinition(
        reason="Helper method used in tests, not part of public API",
        name="file_relative_path",
        filepath=pathlib.Path("great_expectations/data_context/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_table_row_count_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="head",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="expect_column_values_to_not_be_null",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="head",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path("great_expectations/checkpoint/checkpoint.py"),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/datasource/data_connector/runtime_data_connector.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path("great_expectations/datasource/new_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for dict `.update()` method.",
        name="update",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="head",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sparkdf_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Currently only used in testing code, not referenced in docs.",
        name="close",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sqlalchemy_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python `dict`",
        name="dict",
        filepath=pathlib.Path("great_expectations/render/renderer_configuration.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/domain_builder/domain_builder.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/helpers/util.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="False match for context.get_validator()",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/parameter_builder/parameter_builder.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Not used directly but from Data Assistant or RuleBasedProfiler",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/rule/rule.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="self_check is mentioned but in the docs we currently recommend using test_yaml_config which uses self_check under the hood. E.g. https://docs.greatexpectations.io/docs/guides/setup/configuring_data_contexts/how_to_configure_datacontext_components_using_test_yaml_config/#steps",
        name="self_check",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/rule_based_profiler.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="read_csv",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="Exclude code from v2 API",
        name="validate",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="get_metric",
        filepath=pathlib.Path("great_expectations/validator/metrics_calculator.py"),
    ),
    IncludeExcludeDefinition(
        reason="Included in Validator public api",
        name="head",
        filepath=pathlib.Path("great_expectations/validator/metrics_calculator.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python `Set.add()`",
        name="add",
        filepath=pathlib.Path("great_expectations/validator/validation_graph.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for Python dict `.update()`",
        name="update",
        filepath=pathlib.Path("great_expectations/validator/validation_graph.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/core/domain.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/core/expectation_diagnostics/expectation_diagnostics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Internal use",
        name="IDDict",
        filepath=pathlib.Path("great_expectations/core/id_dict.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_mean_to_be_between",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/pandas_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/sparkdf_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="v2 API",
        name="expect_column_values_to_be_in_set",
        filepath=pathlib.Path("great_expectations/dataset/sqlalchemy_dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/expectations/row_conditions.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/attributed_resolved_metrics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/builder.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/config/base.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/estimators/numeric_range_estimation_result.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/estimators/numeric_range_estimator.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/helpers/cardinality_checker.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/helpers/configuration_reconciliation.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/helpers/runtime_environment.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/parameter_container.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/rule/rule.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/types/attributes.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/types/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="Internal helper method",
        name="filter_properties_dict",
        filepath=pathlib.Path("great_expectations/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="to_json_dict is an internal helper method",
        name="to_json_dict",
        filepath=pathlib.Path("great_expectations/validator/exception_info.py"),
    ),
    IncludeExcludeDefinition(
        reason="False match for DataAssistant.run()",
        name="run",
        filepath=pathlib.Path(
            "great_expectations/experimental/rule_based_profiler/data_assistant/data_assistant_runner.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Deprecated v2 api Dataset is not included in the public API",
        filepath=pathlib.Path("great_expectations/dataset/dataset.py"),
    ),
    IncludeExcludeDefinition(
        reason="Validate method on custom type not included in the public API",
        name="validate",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/serializable_types/pyspark.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason='The "columns()" property in this module is not included in the public API',
        name="columns",
        filepath=pathlib.Path("great_expectations/datasource/fluent/sql_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason='The "columns()" property in this module is not included in the public API',
        name="columns",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/spark_generic_partitioners.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/metric_repository.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/data_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="The add method shares a name with a public API method",
        name="add",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/cloud_data_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Metric values are not included in the public API.",
        name="dict",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/metrics.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Internal protocols are not included in the public API.",
        name="add_dataframe_asset",
        filepath=pathlib.Path("great_expectations/core/datasource_dict.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="get_validator",
        filepath=pathlib.Path(
            "great_expectations/experimental/metric_repository/column_descriptive_metrics_metric_retriever.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="Not yet part of the public API",
        name="ResultFormat",
        filepath=pathlib.Path("great_expectations/validator/v1_validator.py"),
    ),
    IncludeExcludeDefinition(
        reason="Not yet part of the public API",
        name="ResultFormat",
        filepath=pathlib.Path("great_expectations/core/result_format.py"),
    ),
    IncludeExcludeDefinition(
        reason="Not yet part of the public API, under active development",
        name="BatchDefinition",
        filepath=pathlib.Path("great_expectations/core/batch_config.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="add_expectation",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="delete_expectation",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/expectations_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="build_batch_request",
        filepath=pathlib.Path("great_expectations/core/batch_config.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="save",
        filepath=pathlib.Path("great_expectations/core/batch_config.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method shares a name with a public API method.",
        name="delete",
        filepath=pathlib.Path(
            "great_expectations/data_context/store/datasource_store.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="",
        name="ExpectColumnValuesToBeInSet",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_be_in_set.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This validate is a name collision with batch.validate().",
        name="validate",
        filepath=pathlib.Path("great_expectations/profile/base.py"),
    ),
    IncludeExcludeDefinition(
        reason="We do not want Expectations in our API docs. Expectation docs live in the gallery.",
        name="ExpectColumnValuesToBeBetween",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_be_between.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="We do not want Expectations in our API docs. Expectation docs live in the gallery.",
        name="ExpectColumnValuesToNotBeNull",
        filepath=pathlib.Path(
            "great_expectations/expectations/core/expect_column_values_to_not_be_null.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users.",
        name="get_or_create_spark_session",
        filepath=pathlib.Path(
            "great_expectations/execution_engine/sparkdf_execution_engine.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method can be removed in 1.0",
        name="get_or_create_spark_application",
        filepath=pathlib.Path("great_expectations/core/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method can be removed in 1.0",
        name="get_or_create_spark_session",
        filepath=pathlib.Path("great_expectations/core/util.py"),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/file_path_data_asset.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/pandas_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path(
            "great_expectations/datasource/fluent/spark_datasource.py"
        ),
    ),
    IncludeExcludeDefinition(
        reason="This method does not need to be accessed by users, and will eventually be removed from docs.",
        name="get_batch_parameters_keys",
        filepath=pathlib.Path("great_expectations/datasource/fluent/sql_datasource.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="OpsgenieAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="PagerdutyAlertAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
    IncludeExcludeDefinition(
        reason="This action is not currently supported",
        name="SNSNotificationAction",
        filepath=pathlib.Path("great_expectations/checkpoint/actions.py"),
    ),
]
