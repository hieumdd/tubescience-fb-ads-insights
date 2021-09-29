from unittest.mock import Mock

import pytest

from main import main

START = "2021-09-10"
END = "2021-09-25"


def run(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res


@pytest.mark.parametrize(
    "ads_account_id",
    [
        "act_1747490262138666",
    ],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=["auto", "manual"],
)
def test_pipelines(ads_account_id, start, end):
    res = run(
        {
            "ads_account_id": ads_account_id,
            "start": start,
            "end": end,
        }
    )
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] == res["num_processed"]
