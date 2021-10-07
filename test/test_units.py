from unittest.mock import Mock

import pytest

from main import main
from tasks import ACCOUNTS

START = "2021-09-01"
END = "2021-10-05"


def run(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res


@pytest.mark.parametrize(
    "table",
    [
        "AdsInsights",
        "VideoInsights",
    ]
)
@pytest.mark.parametrize(
    "ads_account_id",
    ACCOUNTS,
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_pipelines(table, ads_account_id,  start, end):
    res = run(
        {
            "table": table,
            "ads_account_id": ads_account_id,
            "start": start,
            "end": end,
        }
    )
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] == res["num_processed"]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_tasks(start, end):
    res = run(
        {
            "task": "fb",
            "start": start,
            "end": end,
        }
    )
    assert res["tasks"] > 0
