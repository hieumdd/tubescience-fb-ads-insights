import os
import json
from datetime import datetime
import time
from typing import Callable, Optional

import requests

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"

API_VER = "v12.0"
BASE_URL = f"https://graph.facebook.com/{API_VER}/"

DATASET = "Facebook"


class AsyncFailedException(Exception):
    pass


class InsightsFailedException(Exception):
    pass


ReportRunId = str
ReportRunRes = tuple[Optional[Exception], Optional[ReportRunId]]
Insight = dict
Insights = list[Insight]
InsightsRes = tuple[Optional[Exception], Optional[Insights]]


def request_async_report(
    fields: list[str],
    windows: list[str],
    breakdowns: str = None,
) -> Callable[[requests.Session, str, datetime, datetime], ReportRunRes,]:
    def send(
        session: requests.Session,
        ads_account_id: str,
        start: datetime,
        end: datetime,
    ) -> ReportRunRes:
        params = {
            "access_token": os.getenv("ACCESS_TOKEN"),
            "level": "ad",
            "fields": json.dumps(fields),
            "action_attribution_windows": json.dumps(windows),
            "filtering": json.dumps(
                [
                    {
                        "field": "ad.impressions",
                        "operator": "GREATER_THAN",
                        "value": 0,
                    },
                    {
                        "field": "ad.effective_status",
                        "operator": "IN",
                        "value": [
                            "ACTIVE",
                            "PAUSED",
                            "DELETED",
                            "PENDING_REVIEW",
                            "DISAPPROVED",
                            "PREAPPROVED",
                            "PENDING_BILLING_INFO",
                            "CAMPAIGN_PAUSED",
                            "ARCHIVED",
                            "ADSET_PAUSED",
                            "IN_PROCESS",
                            "WITH_ISSUES",
                        ],
                    },
                ]
            ),
            "time_increment": 1,
            "time_range": json.dumps(
                {
                    "since": start.strftime(DATE_FORMAT),
                    "until": end.strftime(DATE_FORMAT),
                }
            ),
        }
        if breakdowns:
            params["breakdowns"] = breakdowns
        try:
            with session.post(
                f"{BASE_URL}/{ads_account_id}/insights", params=params
            ) as r:
                res = r.json()
            return None, res["report_run_id"]
        except KeyError as e:
            return e, None

    return send


def poll_async_report(session: requests.Session, report_run_id: str) -> ReportRunRes:
    try:
        with session.get(
            f"{BASE_URL}/{report_run_id}",
            params={"access_token": os.getenv("ACCESS_TOKEN")},
        ) as r:
            res = r.json()
        if (
            res["async_percent_completion"] == 100
            and res["async_status"] == "Job Completed"
        ):
            return None, report_run_id
        elif res["async_status"] == "Job Failed":
            return AsyncFailedException(report_run_id), None
        else:
            time.sleep(10)
            return poll_async_report(session, report_run_id)
    except Exception as e:
        return e, None


def get_async_report(
    session: requests.Session,
    requester: Callable[
        [requests.Session, str, datetime, datetime],
        ReportRunRes,
    ],
    ads_account_id: str,
    start: datetime,
    end: datetime,
) -> ReportRunRes:
    err_request, request = requester(session, ads_account_id, start, end)
    if not err_request and request:
        err_polled_request, polled_request = poll_async_report(session, request)
        if err_polled_request and isinstance(err_polled_request, AsyncFailedException):
            return get_async_report(session, requester, ads_account_id, start, end)
        else:
            return err_polled_request, polled_request
    else:
        return err_request, request


def get_insights(
    session: requests.Session,
    report_run_id: ReportRunId,
    after: str = None,
) -> InsightsRes:
    def get() -> list[dict]:
        with session.get(
            f"{BASE_URL}/{report_run_id}/insights",
            params={
                "access_token": os.getenv("ACCESS_TOKEN"),
                "limit": 500,
                "after": after,
            },
        ) as r:
            res = r.json()
        data = res["data"]
        next_ = res["paging"].get("next")
        return (
            data
            + get_insights(session, report_run_id, res["paging"]["cursors"]["after"])
            if next_
            else data
        )

    try:
        return None, get()
    except Exception as e:
        return e, None


def transform_add_batched_at(row: Insight):
    return {
        **row,
        "_batched_at": NOW.isoformat(timespec="seconds"),
    }
