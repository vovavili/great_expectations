import shutil

import pytest

from great_expectations.data_context import get_context
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import InvalidConfigError


@pytest.mark.filesystem
def test_incomplete_uncommitted(tmp_path):
    """
    When a project is shared between users, it is common to have an incomplete
    uncommitted directory present. We should fail gracefully when config
    variables are missing.
    """
    local_dir = tmp_path / "root"
    fixture_path = file_relative_path(
        __file__,
        "./fixtures/contexts/incomplete_uncommitted/great_expectations",
    )
    shutil.copytree(fixture_path, local_dir)

    with pytest.raises(InvalidConfigError) as exc:
        _ = get_context(context_root_dir=local_dir)

    assert (
        "Unable to find a match for config substitution variable: "
        "`secret_validation_results_store_name`." in exc.value.message
    )
    assert (
        "See https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_credentials"
        in exc.value.message
    )
