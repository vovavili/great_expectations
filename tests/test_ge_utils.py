import copy
import datetime
import os
from typing import TYPE_CHECKING, Any

import pytest

import great_expectations as gx
from great_expectations.core.util import nested_update
from great_expectations.util import (
    convert_ndarray_datetime_to_float_dtype_utc_timezone,
    convert_ndarray_float_to_datetime_tuple,
    convert_ndarray_to_datetime_dtype_best_effort,
    deep_filter_properties_iterable,
    filter_properties_dict,
    hyphen,
    is_ndarray_datetime_dtype,
)

if TYPE_CHECKING:
    import numpy as np


@pytest.fixture
def datetime_array():
    week_idx: int
    return [
        datetime.datetime(2021, 1, 1, 0, 0, 0) + datetime.timedelta(days=(week_idx * 7))  # noqa: DTZ001
        for week_idx in range(4)
    ]


@pytest.fixture
def datetime_string_array():
    week_idx: int
    return [
        (
            datetime.datetime(2021, 1, 1, 0, 0, 0) + datetime.timedelta(days=(week_idx * 7))  # noqa: DTZ001
        ).isoformat()
        for week_idx in range(4)
    ]


@pytest.fixture
def numeric_array():
    idx: int
    return [idx for idx in range(4)]


@pytest.mark.unit
def test_gen_directory_tree_str(tmpdir):
    project_dir = str(tmpdir.mkdir("project_dir"))
    os.mkdir(os.path.join(project_dir, "BBB"))  # noqa: PTH102, PTH118
    with open(os.path.join(project_dir, "BBB", "bbb.txt"), "w") as f:  # noqa: PTH118
        f.write("hello")
    with open(os.path.join(project_dir, "BBB", "aaa.txt"), "w") as f:  # noqa: PTH118
        f.write("hello")

    os.mkdir(os.path.join(project_dir, "AAA"))  # noqa: PTH102, PTH118

    res = gx.util.gen_directory_tree_str(project_dir)
    print(res)

    # Note: files and directories are sorteds alphabetically, so that this method can be used for testing.  # noqa: E501
    assert (
        res
        == """\
project_dir/
    AAA/
    BBB/
        aaa.txt
        bbb.txt
"""
    )


@pytest.mark.unit
def test_nested_update():
    # nested_update is useful for update nested dictionaries (such as batch_kwargs with reader_options as a dictionary)  # noqa: E501
    batch_kwargs = {
        "path": "/a/path",
        "reader_method": "read_csv",
        "reader_options": {"header": 0},
    }

    nested_update(batch_kwargs, {"reader_options": {"nrows": 1}})

    assert batch_kwargs == {
        "path": "/a/path",
        "reader_method": "read_csv",
        "reader_options": {"header": 0, "nrows": 1},
    }


@pytest.mark.unit
def test_nested_update_lists():
    # nested_update is useful for update nested dictionaries (such as batch_kwargs with reader_options as a dictionary)  # noqa: E501
    dependencies = {
        "suite.warning": {"metric.name": ["column=foo"]},
        "suite.failure": {"metric.blarg": [""]},
    }

    new_dependencies = {
        "suite.warning": {
            "metric.other_name": ["column=foo"],
            "metric.name": ["column=bar"],
        }
    }

    nested_update(dependencies, new_dependencies)

    assert dependencies == {
        "suite.warning": {
            "metric.name": ["column=foo", "column=bar"],
            "metric.other_name": ["column=foo"],
        },
        "suite.failure": {"metric.blarg": [""]},
    }


@pytest.mark.unit
def test_filter_properties_dict():
    source_dict: dict = {
        "integer_zero": 0,
        "null": None,
        "string": "xyz_0",
        "integer_one": 1,
        "scientific_notation_floating_point_number": 9.8e1,
    }

    d0_begin: dict = copy.deepcopy(source_dict)
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        d0_end: dict = filter_properties_dict(
            properties=d0_begin,
            keep_fields={
                "string",
            },
            delete_fields={
                "integer_zero",
                "scientific_notation_floating_point_number",
                "string",
            },
            clean_falsy=True,
        )
    d0_end: dict = filter_properties_dict(properties=d0_begin, clean_falsy=True)
    d0_end_expected: dict = copy.deepcopy(d0_begin)
    d0_end_expected.pop("null")
    assert d0_end == d0_end_expected

    d1_begin: dict = copy.deepcopy(source_dict)
    d1_end: dict = filter_properties_dict(
        properties=d1_begin,
        clean_nulls=False,
    )
    d1_end_expected: dict = d1_begin
    assert d1_end == d1_end_expected

    d2_begin: dict = copy.deepcopy(source_dict)
    d2_end: dict = filter_properties_dict(
        properties=d2_begin,
        clean_nulls=True,
        clean_falsy=False,
    )
    d2_end_expected: dict = copy.deepcopy(d2_begin)
    d2_end_expected.pop("null")
    assert d2_end == d2_end_expected

    d3_begin: dict = copy.deepcopy(source_dict)
    d3_end: dict = filter_properties_dict(
        properties=d3_begin,
        keep_fields={
            "null",
        },
        clean_falsy=True,
    )
    d3_end_expected: dict = {"null": None}
    assert d3_end == d3_end_expected

    d4_begin: dict = copy.deepcopy(source_dict)
    d4_end: dict = filter_properties_dict(
        properties=d4_begin,
        clean_falsy=True,
        keep_falsy_numerics=False,
    )
    d4_end_expected: dict = copy.deepcopy(d4_begin)
    d4_end_expected.pop("integer_zero")
    d4_end_expected.pop("null")
    assert d4_end == d4_end_expected

    d5_begin: dict = copy.deepcopy(source_dict)
    d5_end: dict = filter_properties_dict(
        properties=d5_begin,
        keep_fields={
            "integer_zero",
            "scientific_notation_floating_point_number",
        },
        clean_falsy=True,
    )
    d5_end_expected: dict = {
        "integer_zero": 0,
        "scientific_notation_floating_point_number": 9.8e1,
    }
    assert d5_end == d5_end_expected

    d6_begin: dict = copy.deepcopy(source_dict)
    d6_end: dict = filter_properties_dict(
        properties=d6_begin,
        delete_fields={
            "integer_zero",
            "scientific_notation_floating_point_number",
        },
        clean_falsy=True,
    )
    d6_end_expected: dict = {"string": "xyz_0", "integer_one": 1}
    assert d6_end == d6_end_expected

    d7_begin: dict = copy.deepcopy(source_dict)
    filter_properties_dict(
        properties=d7_begin,
        delete_fields={
            "integer_zero",
            "scientific_notation_floating_point_number",
        },
        clean_falsy=True,
        inplace=True,
    )
    d7_end: dict = d7_begin
    d7_end_expected: dict = {"string": "xyz_0", "integer_one": 1}
    assert d7_end == d7_end_expected


@pytest.mark.unit
def test_deep_filter_properties_iterable():
    source_dict: dict = {
        "integer_zero": 0,
        "null": None,
        "string": "xyz_0",
        "integer_one": 1,
        "scientific_notation_floating_point_number": 9.8e1,
        "empty_top_level_dictionary": {},
        "empty_top_level_list": [],
        "empty_top_level_set": set(),
        "non_empty_top_level_set": {
            0,
            1,
            2,
            "a",
            "b",
            "c",
        },
        "non_empty_top_level_dictionary": {
            "empty_1st_level_list": [],
            "empty_1st_level_set": set(),
            "non_empty_1st_level_set": {
                "empty_2nd_level_list": [],
                "non_empty_2nd_level_list": [
                    0,
                    1,
                    2,
                    "a",
                    "b",
                    "c",
                ],
                "non_empty_2nd_level_dictionary": {
                    "integer_zero": 0,
                    "null": None,
                    "string": "xyz_0",
                    "integer_one": 1,
                    "scientific_notation_floating_point_number": 9.8e1,
                },
                "empty_2nd_level_dictionary": {},
            },
        },
    }

    d0_begin: dict = copy.deepcopy(source_dict)
    deep_filter_properties_iterable(
        properties=d0_begin,
        clean_falsy=True,
        inplace=True,
    )
    d0_end: dict = d0_begin
    d0_end_expected: dict = {
        "integer_zero": 0,
        "string": "xyz_0",
        "integer_one": 1,
        "scientific_notation_floating_point_number": 98.0,
        "non_empty_top_level_set": {
            0,
            1,
            2,
            "a",
            "b",
            "c",
        },
        "non_empty_top_level_dictionary": {
            "non_empty_1st_level_set": {
                "non_empty_2nd_level_list": [0, 1, 2, "a", "b", "c"],
                "non_empty_2nd_level_dictionary": {
                    "integer_zero": 0,
                    "string": "xyz_0",
                    "integer_one": 1,
                    "scientific_notation_floating_point_number": 98.0,
                },
            }
        },
    }
    assert d0_end == d0_end_expected

    d1_begin: dict = copy.deepcopy(source_dict)
    d1_end: dict = deep_filter_properties_iterable(
        properties=d1_begin,
        clean_falsy=True,
        keep_falsy_numerics=False,
    )
    d1_end_expected: dict = {
        "string": "xyz_0",
        "integer_one": 1,
        "scientific_notation_floating_point_number": 98.0,
        "non_empty_top_level_set": {
            0,
            1,
            2,
            "a",
            "b",
            "c",
        },
        "non_empty_top_level_dictionary": {
            "non_empty_1st_level_set": {
                "non_empty_2nd_level_list": [0, 1, 2, "a", "b", "c"],
                "non_empty_2nd_level_dictionary": {
                    "string": "xyz_0",
                    "integer_one": 1,
                    "scientific_notation_floating_point_number": 98.0,
                },
            }
        },
    }
    assert d1_end == d1_end_expected


@pytest.mark.unit
def test_deep_filter_properties_iterable_on_batch_request_dict():
    batch_request: dict = {
        "datasource_name": "df78ebde1957385a02d8736cd2c9a6d9",
        "data_connector_name": "123a3221fc4b65014d061cce4a71782e",
        "data_asset_name": "eac128c5824b698c22b441ada61022d4",
        "batch_spec_passthrough": {},
        "data_connector_query": {"batch_filter_parameters": {}},
        "limit": None,
    }

    deep_filter_properties_iterable(
        properties=batch_request, clean_nulls=True, clean_falsy=True, inplace=True
    )

    assert batch_request == {
        "datasource_name": "df78ebde1957385a02d8736cd2c9a6d9",
        "data_connector_name": "123a3221fc4b65014d061cce4a71782e",
        "data_asset_name": "eac128c5824b698c22b441ada61022d4",
    }


@pytest.mark.unit
def test_is_ndarray_datetime_dtype(
    datetime_array,
    datetime_string_array,
    numeric_array,
):
    assert is_ndarray_datetime_dtype(
        data=datetime_array, parse_strings_as_datetimes=False, fuzzy=False
    )
    assert is_ndarray_datetime_dtype(
        data=datetime_array, parse_strings_as_datetimes=True, fuzzy=False
    )

    assert not is_ndarray_datetime_dtype(
        data=datetime_string_array, parse_strings_as_datetimes=False, fuzzy=False
    )
    assert is_ndarray_datetime_dtype(
        data=datetime_string_array, parse_strings_as_datetimes=True, fuzzy=False
    )

    assert not is_ndarray_datetime_dtype(
        data=numeric_array, parse_strings_as_datetimes=False, fuzzy=False
    )
    assert not is_ndarray_datetime_dtype(
        data=numeric_array, parse_strings_as_datetimes=True, fuzzy=False
    )

    datetime_string_array[-1] = "malformed_datetime_string"
    assert not is_ndarray_datetime_dtype(
        data=datetime_string_array, parse_strings_as_datetimes=True, fuzzy=False
    )


@pytest.mark.unit
def test_convert_ndarray_to_datetime_dtype_best_effort(
    datetime_array,
    datetime_string_array,
    numeric_array,
):
    original_ndarray_is_datetime_type: bool
    conversion_ndarray_to_datetime_type_performed: bool
    ndarray_is_datetime_type: bool
    values_converted: np.ndaarray

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=datetime_array,
        parse_strings_as_datetimes=False,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert ndarray_is_datetime_type
    assert values_converted == datetime_array

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=datetime_array,
        parse_strings_as_datetimes=True,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert ndarray_is_datetime_type
    assert values_converted == datetime_array

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=datetime_string_array,
        parse_strings_as_datetimes=False,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert not ndarray_is_datetime_type
    assert values_converted == datetime_string_array

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=datetime_string_array,
        parse_strings_as_datetimes=True,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert ndarray_is_datetime_type
    assert values_converted.tolist() == datetime_array

    datetime_string_array[-1] = "malformed_datetime_string"
    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=datetime_string_array,
        parse_strings_as_datetimes=True,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert not ndarray_is_datetime_type
    assert values_converted == datetime_string_array

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=numeric_array,
        parse_strings_as_datetimes=False,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert not ndarray_is_datetime_type
    assert values_converted == numeric_array

    (
        original_ndarray_is_datetime_type,
        conversion_ndarray_to_datetime_type_performed,
        values_converted,
    ) = convert_ndarray_to_datetime_dtype_best_effort(
        data=numeric_array,
        parse_strings_as_datetimes=True,
    )
    ndarray_is_datetime_type = (
        original_ndarray_is_datetime_type or conversion_ndarray_to_datetime_type_performed
    )

    assert not ndarray_is_datetime_type
    assert values_converted == numeric_array


@pytest.mark.unit
def test_convert_ndarray_datetime_to_float_dtype_utc_timezone(
    datetime_array,
    datetime_string_array,
    numeric_array,
):
    element: Any
    assert convert_ndarray_datetime_to_float_dtype_utc_timezone(data=datetime_array).tolist() == [
        element.replace(tzinfo=datetime.timezone.utc).timestamp() for element in datetime_array
    ]

    with pytest.raises(AttributeError) as e:
        _ = convert_ndarray_datetime_to_float_dtype_utc_timezone(data=numeric_array)

    assert "'int' object has no attribute 'replace'" in str(e.value)

    with pytest.raises(TypeError) as e:
        _ = convert_ndarray_datetime_to_float_dtype_utc_timezone(data=datetime_string_array)

    assert "replace() takes no keyword arguments" in str(e.value)


@pytest.mark.unit
def test_convert_ndarray_float_to_datetime_tuple(
    datetime_array,
):
    element: Any
    assert convert_ndarray_float_to_datetime_tuple(
        data=[
            element.replace(tzinfo=datetime.timezone.utc).timestamp() for element in datetime_array
        ]
    ) == tuple([element for element in datetime_array])

    with pytest.raises(TypeError) as e:
        _ = convert_ndarray_float_to_datetime_tuple(data=datetime_array)

    # Error message varies based on version but mainly looking to validate type error by not using integer  # noqa: E501
    assert all(string in str(e.value) for string in ("datetime.datetime", "integer"))


@pytest.mark.unit
def test_hyphen():
    txt: str = "validation_result"
    assert hyphen(txt=txt) == "validation-result"
