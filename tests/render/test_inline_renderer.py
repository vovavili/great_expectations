from typing import List

import pytest

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    RenderedAtomicContent,
)
from great_expectations.render.exceptions import InlineRendererError
from great_expectations.render.renderer.inline_renderer import InlineRenderer
from great_expectations.render.renderer_configuration import MetaNotesFormat

# module level markers
pytestmark = pytest.mark.unit


def clean_serialized_rendered_atomic_content_graphs(
    serialized_rendered_atomic_content: List[dict],
) -> List[dict]:
    for content_block in serialized_rendered_atomic_content:
        if content_block["value_type"] == "GraphType":
            content_block["value"]["graph"].pop("$schema")
            content_block["value"]["graph"].pop("data")
            content_block["value"]["graph"].pop("datasets")

    return serialized_rendered_atomic_content


def test_inline_renderer_instantiation_error_message(
    basic_expectation_suite: ExpectationSuite,
):
    expectation_suite: ExpectationSuite = basic_expectation_suite
    with pytest.raises(InlineRendererError) as e:
        InlineRenderer(render_object=expectation_suite)  # type: ignore
    assert (
        str(e.value)
        == "InlineRenderer can only be used with an ExpectationConfiguration or ExpectationValidationResult, but <class 'great_expectations.core.expectation_suite.ExpectationSuite'> was used."  # noqa: E501
    )


@pytest.mark.parametrize(
    "expectation_configuration,fake_result,expected_serialized_expectation_validation_result_rendered_atomic_content",
    [
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
            ),
            {"observed_value": 3},
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "3",
                    },
                    "value_type": "StringValueType",
                },
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                    },
                },
            ],
            id="equal",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_min_to_be_between",
                kwargs={"column": "event_type", "min_value": 3, "max_value": 20},
            ),
            {"observed_value": 19},
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "19",
                    },
                    "value_type": "StringValueType",
                },
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "max_value": {"schema": {"type": "number"}, "value": 20},
                            "min_value": {"schema": {"type": "number"}, "value": 3},
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column minimum value must be greater than or equal "
                        "to $min_value and less than or equal to $max_value.",
                    },
                    "value_type": "StringValueType",
                },
            ],
            id="between",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_quantile_values_to_be_between",
                kwargs={
                    "column": "user_id",
                    "quantile_ranges": {
                        "quantiles": [0.0, 0.5, 1.0],
                        "value_ranges": [
                            [300000, 400000],
                            [2000000, 4000000],
                            [4000000, 10000000],
                        ],
                    },
                },
            ),
            {
                "observed_value": {
                    "quantiles": [0.0, 0.5, 1.0],
                    "values": [397433, 2388055, 9488404],
                },
                "details": {"success_details": [True, True, True]},
            },
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "header_row": [
                            {"schema": {"type": "string"}, "value": "Quantile"},
                            {"schema": {"type": "string"}, "value": "Value"},
                        ],
                        "schema": {"type": "TableType"},
                        "table": [
                            [
                                {"schema": {"type": "string"}, "value": "0.00"},
                                {"schema": {"type": "number"}, "value": 397433},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "Median"},
                                {"schema": {"type": "number"}, "value": 2388055},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "1.00"},
                                {"schema": {"type": "number"}, "value": 9488404},
                            ],
                        ],
                    },
                    "value_type": "TableType",
                },
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                },
                                "template": "$column quantiles must be within "
                                "the following value ranges.",
                            },
                        },
                        "header_row": [
                            {"schema": {"type": "string"}, "value": "Quantile"},
                            {"schema": {"type": "string"}, "value": "Min Value"},
                            {"schema": {"type": "string"}, "value": "Max Value"},
                        ],
                        "schema": {"type": "TableType"},
                        "table": [
                            [
                                {"schema": {"type": "string"}, "value": "0.00"},
                                {"schema": {"type": "number"}, "value": 300000},
                                {"schema": {"type": "number"}, "value": 400000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "Median"},
                                {"schema": {"type": "number"}, "value": 2000000},
                                {"schema": {"type": "number"}, "value": 4000000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "1.00"},
                                {"schema": {"type": "number"}, "value": 4000000},
                                {"schema": {"type": "number"}, "value": 10000000},
                            ],
                        ],
                    },
                    "value_type": "TableType",
                },
            ],
            id="table",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_values_to_be_in_set",
                kwargs={"column": "event_type", "value_set": [19, 22, 73]},
            ),
            {
                "element_count": 3,
                "unexpected_count": 0,
                "unexpected_percent": 0.0,
                "partial_unexpected_list": [],
                "missing_count": 0,
                "missing_percent": 0.0,
                "unexpected_percent_total": 0.0,
                "unexpected_percent_nonmissing": 0.0,
            },
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "params": {},
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "0% unexpected",
                    },
                    "value_type": "StringValueType",
                },
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "v__0": {"schema": {"type": "number"}, "value": 19},
                            "v__1": {"schema": {"type": "number"}, "value": 22},
                            "v__2": {"schema": {"type": "number"}, "value": 73},
                            "value_set": {
                                "schema": {"type": "array"},
                                "value": [19, 22, 73],
                            },
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column values must belong to this set: $v__0 $v__1 " "$v__2.",
                    },
                    "value_type": "StringValueType",
                },
            ],
            id="set",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_kl_divergence_to_be_less_than",
                kwargs={
                    "column": "user_id",
                    "partition_object": {
                        "values": [2000000, 6000000],
                        "weights": [0.3, 0.7],
                    },
                },
            ),
            {
                "observed_value": None,
                "details": {
                    "observed_partition": {
                        "values": [2000000, 6000000, 397433, 2388055, 9488404],
                        "weights": [
                            0.0,
                            0.0,
                            0.3333333333333333,
                            0.3333333333333333,
                            0.3333333333333333,
                        ],
                    },
                    "expected_partition": {
                        "values": [2000000, 6000000, 397433, 2388055, 9488404],
                        "weights": [0.3, 0.7, 0.0, 0.0, 0.0],
                    },
                },
            },
            [
                {
                    "name": AtomicDiagnosticRendererType.OBSERVED_VALUE,
                    "value": {
                        "graph": {
                            "autosize": "fit",
                            "config": {
                                "view": {
                                    "continuousHeight": 300,
                                    "continuousWidth": 400,
                                }
                            },
                            "encoding": {
                                "tooltip": [
                                    {"field": "values", "type": "quantitative"},
                                    {"field": "fraction", "type": "quantitative"},
                                ],
                                "x": {"field": "values", "type": "nominal"},
                                "y": {"field": "fraction", "type": "quantitative"},
                            },
                            "height": 400,
                            "mark": "bar",
                            "width": 250,
                        },
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "observed_value": {
                                        "schema": {"type": "string"},
                                        "value": "None (-infinity, infinity, or NaN)",
                                    }
                                },
                                "template": "KL Divergence: $observed_value",
                            },
                        },
                        "schema": {"type": "GraphType"},
                    },
                    "value_type": "GraphType",
                },
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "graph": {
                            "autosize": "fit",
                            "config": {
                                "view": {
                                    "continuousHeight": 300,
                                    "continuousWidth": 400,
                                }
                            },
                            "encoding": {
                                "tooltip": [
                                    {"field": "values", "type": "quantitative"},
                                    {"field": "fraction", "type": "quantitative"},
                                ],
                                "x": {"field": "values", "type": "nominal"},
                                "y": {"field": "fraction", "type": "quantitative"},
                            },
                            "height": 400,
                            "mark": "bar",
                            "width": 250,
                        },
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                },
                                "template": "$column Kullback-Leibler (KL) "
                                "divergence with respect to the "
                                "following distribution must be "
                                "lower than $threshold.",
                            },
                        },
                        "schema": {"type": "GraphType"},
                    },
                    "value_type": "GraphType",
                },
            ],
            id="graph",
        ),
    ],
)
def test_inline_renderer_expectation_validation_result_serialization(
    expectation_configuration: ExpectationConfiguration,
    fake_result: dict,
    expected_serialized_expectation_validation_result_rendered_atomic_content: dict,
):
    expectation_validation_result = ExpectationValidationResult(
        exception_info={
            "raised_exception": False,
            "exception_traceback": None,
            "exception_message": None,
        },
        expectation_config=expectation_configuration,
        result=fake_result,
        success=True,
    )

    inline_renderer: InlineRenderer = InlineRenderer(render_object=expectation_validation_result)

    expectation_validation_result_rendered_atomic_content: List[RenderedAtomicContent] = (
        inline_renderer.get_rendered_content()
    )

    actual_serialized_expectation_validation_result_rendered_atomic_content: List[dict] = (
        clean_serialized_rendered_atomic_content_graphs(
            serialized_rendered_atomic_content=[
                rendered_atomic_content.to_json_dict()
                for rendered_atomic_content in expectation_validation_result_rendered_atomic_content
            ]
        )
    )

    assert (
        actual_serialized_expectation_validation_result_rendered_atomic_content
        == expected_serialized_expectation_validation_result_rendered_atomic_content
    )


@pytest.mark.parametrize(
    "expectation_configuration,expected_serialized_expectation_configuration_rendered_atomic_content",
    [
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                    },
                }
            ],
            id="equal",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_min_to_be_between",
                kwargs={"column": "event_type", "min_value": 3, "max_value": 20},
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "max_value": {"schema": {"type": "number"}, "value": 20},
                            "min_value": {"schema": {"type": "number"}, "value": 3},
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column minimum value must be greater than or equal "
                        "to $min_value and less than or equal to $max_value.",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="between",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_quantile_values_to_be_between",
                kwargs={
                    "column": "user_id",
                    "quantile_ranges": {
                        "quantiles": [0.0, 0.5, 1.0],
                        "value_ranges": [
                            [300000, 400000],
                            [2000000, 4000000],
                            [4000000, 10000000],
                        ],
                    },
                },
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                },
                                "template": "$column quantiles must be within "
                                "the following value ranges.",
                            },
                        },
                        "header_row": [
                            {"schema": {"type": "string"}, "value": "Quantile"},
                            {"schema": {"type": "string"}, "value": "Min Value"},
                            {"schema": {"type": "string"}, "value": "Max Value"},
                        ],
                        "schema": {"type": "TableType"},
                        "table": [
                            [
                                {"schema": {"type": "string"}, "value": "0.00"},
                                {"schema": {"type": "number"}, "value": 300000},
                                {"schema": {"type": "number"}, "value": 400000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "Median"},
                                {"schema": {"type": "number"}, "value": 2000000},
                                {"schema": {"type": "number"}, "value": 4000000},
                            ],
                            [
                                {"schema": {"type": "string"}, "value": "1.00"},
                                {"schema": {"type": "number"}, "value": 4000000},
                                {"schema": {"type": "number"}, "value": 10000000},
                            ],
                        ],
                    },
                    "value_type": "TableType",
                }
            ],
            id="table",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_values_to_be_in_set",
                kwargs={"column": "event_type", "value_set": [19, 22, 73]},
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "params": {
                            "column": {
                                "schema": {"type": "string"},
                                "value": "event_type",
                            },
                            "v__0": {"schema": {"type": "number"}, "value": 19},
                            "v__1": {"schema": {"type": "number"}, "value": 22},
                            "v__2": {"schema": {"type": "number"}, "value": 73},
                            "value_set": {
                                "schema": {"type": "array"},
                                "value": [19, 22, 73],
                            },
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column values must belong to this set: $v__0 $v__1 " "$v__2.",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="set",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_kl_divergence_to_be_less_than",
                kwargs={
                    "column": "user_id",
                    "partition_object": {
                        "values": [2000000, 6000000],
                        "weights": [0.3, 0.7],
                    },
                },
            ),
            [
                {
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "graph": {
                            "autosize": "fit",
                            "config": {
                                "view": {
                                    "continuousHeight": 300,
                                    "continuousWidth": 400,
                                }
                            },
                            "encoding": {
                                "tooltip": [
                                    {"field": "values", "type": "quantitative"},
                                    {"field": "fraction", "type": "quantitative"},
                                ],
                                "x": {"field": "values", "type": "nominal"},
                                "y": {"field": "fraction", "type": "quantitative"},
                            },
                            "height": 400,
                            "mark": "bar",
                            "width": 250,
                        },
                        "header": {
                            "schema": {"type": "StringValueType"},
                            "value": {
                                "params": {
                                    "column": {
                                        "schema": {"type": "string"},
                                        "value": "user_id",
                                    },
                                },
                                "template": "$column Kullback-Leibler (KL) "
                                "divergence with respect to the "
                                "following distribution must be "
                                "lower than $threshold.",
                            },
                        },
                        "schema": {"type": "GraphType"},
                    },
                    "value_type": "GraphType",
                }
            ],
            id="graph",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
                meta={
                    "notes": {
                        "format": MetaNotesFormat.STRING,
                        "content": ["This is the most important Expectation!!"],
                    }
                },
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                        "meta_notes": {
                            "content": ["This is the most important Expectation!!"],
                            "format": MetaNotesFormat.STRING,
                        },
                    },
                }
            ],
            id="meta_notes content list",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
                meta={
                    "notes": {
                        "format": MetaNotesFormat.STRING,
                        "content": "This is the most important Expectation!!",
                    }
                },
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                        "meta_notes": {
                            "content": ["This is the most important Expectation!!"],
                            "format": MetaNotesFormat.STRING,
                        },
                    },
                }
            ],
            id="meta_notes content string",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                kwargs={"value": 3},
                meta={
                    "notes": "This is the most important Expectation!!",
                },
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Must have exactly $value rows.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 3},
                        },
                        "meta_notes": {
                            "content": ["This is the most important Expectation!!"],
                            "format": MetaNotesFormat.STRING,
                        },
                    },
                }
            ],
            id="meta_notes string",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_table_row_count_to_equal",
                description="Row count must be equal to "
                "the meaning of life, the universe, and everything.",
                kwargs={"value": 42},
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "Row count must be equal to "
                        "the meaning of life, the universe, and everything.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "value": {"schema": {"type": "number"}, "value": 42},
                        },
                    },
                }
            ],
            id="description",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_values_to_be_between",
                kwargs={
                    "column": "column_a",
                    "min_value": 0,
                    "max_value": 10,
                    "row_condition": 'col("column_b")==5',
                    "condition_parser": "great_expectations",
                },
            ),
            [
                {
                    "value_type": "StringValueType",
                    "name": AtomicPrescriptiveRendererType.SUMMARY,
                    "value": {
                        "template": "If "
                        'col("$row_condition__0")$row_condition__1, '
                        "then $column values must be greater than or equal to "
                        "$min_value and less than or equal to $max_value.",
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "params": {
                            "column": {"schema": {"type": "string"}, "value": "column_a"},
                            "min_value": {"schema": {"type": "number"}, "value": 0},
                            "max_value": {"schema": {"type": "number"}, "value": 10},
                            "row_condition__0": {"schema": {"type": "string"}, "value": "column_b"},
                            "row_condition__1": {"schema": {"type": "string"}, "value": "==5"},
                        },
                    },
                }
            ],
            id="row_condition",
        ),
        pytest.param(
            ExpectationConfiguration(
                type="expect_column_value_z_scores_to_be_less_than",
                kwargs={"column": "column_a", "threshold": 4, "double_sided": True},
            ),
            [
                {
                    "name": "atomic.prescriptive.summary",
                    "value": {
                        "params": {
                            "column": {"schema": {"type": "string"}, "value": "column_a"},
                            "threshold": {"schema": {"type": "number"}, "value": 4},
                            "inverse_threshold": {"schema": {"type": "number"}, "value": -4},
                        },
                        "schema": {"type": "com.superconductive.rendered.string"},
                        "template": "$column value z-scores must be greater "
                        "than $inverse_threshold and less than $threshold.",
                    },
                    "value_type": "StringValueType",
                }
            ],
            id="z_score_double_sided",
        ),
    ],
)
def test_inline_renderer_expectation_configuration_serialization(
    expectation_configuration: ExpectationConfiguration,
    expected_serialized_expectation_configuration_rendered_atomic_content: dict,
):
    inline_renderer: InlineRenderer = InlineRenderer(render_object=expectation_configuration)

    expectation_configuration_rendered_atomic_content: List[RenderedAtomicContent] = (
        inline_renderer.get_rendered_content()
    )

    actual_serialized_expectation_configuration_rendered_atomic_content: List[dict] = (
        clean_serialized_rendered_atomic_content_graphs(
            serialized_rendered_atomic_content=[
                rendered_atomic_content.to_json_dict()
                for rendered_atomic_content in expectation_configuration_rendered_atomic_content
            ]
        )
    )

    assert (
        actual_serialized_expectation_configuration_rendered_atomic_content
        == expected_serialized_expectation_configuration_rendered_atomic_content
    )
