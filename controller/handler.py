from datetime import datetime, timedelta
from typing import Optional

import requests
from google.cloud import bigquery

from controller.facebook import get_async_report, get_insights, transform_add_batched_at
from controller.bigquery import load

NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"


def run(
    client: bigquery.Client,
    session: requests.Session,
    model,
    ads_account_id: str,
    start: Optional[str],
    end: Optional[str],
) -> tuple[Optional[Exception], Optional[dict]]:
    _start = (
        (NOW - timedelta(days=8))
        if not start
        else datetime.strptime(start, DATE_FORMAT)
    )
    _end = NOW if not end else datetime.strptime(end, DATE_FORMAT)
    err_report_id, report_id = get_async_report(
        session,
        model["requester"],
        ads_account_id,
        _start,
        _end,
    )
    if report_id:
        err_data, data = get_insights(session, report_id)
        if data:
            return None, {
                "ads_account_id": ads_account_id,
                "start": start,
                "end": end,
                "num_processed": len(data),
                "output_rows": load(
                    client,
                    transform_add_batched_at(model["transform"](data)),
                    model["table"],
                    model["schema"],
                    model["keys"],
                ),
            }
        if err_data:
            return err_data, None
    return err_report_id, None
