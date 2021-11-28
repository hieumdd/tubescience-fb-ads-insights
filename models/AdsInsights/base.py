from typing import Callable, Optional
import os
from datetime import datetime, timedelta


from libs.facebook import Insights, AsyncRequest, get
from libs.bigquery import Load

AdsInsightsPipeline = Callable[[str, Optional[str], Optional[str]], dict]

DATE_FORMAT = "%Y-%m-%d"


def transform_add_batched_at(rows: Insights) -> Insights:
    return [
        {
            **row,
            "_batched_at": datetime.utcnow().isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def get_time_range(
    start: Optional[str],
    end: Optional[str],
) -> tuple[datetime, datetime]:
    return (
        (datetime.utcnow() - timedelta(days=8))
        if not start
        else datetime.strptime(start, DATE_FORMAT)
    ), datetime.utcnow() if not end else datetime.strptime(end, DATE_FORMAT)


def ads_insights_pipeline(
    async_request: AsyncRequest,
    transform: Callable[[list[dict]], list[dict]],
    load: Load,
) -> AdsInsightsPipeline:
    def run(ads_account_id: str, start: Optional[str], end: Optional[str]) -> dict:
        data = get(async_request, ads_account_id, *get_time_range(start, end))
        response = {
            "ads_account_id": ads_account_id,
            "start": start,
            "end": end,
            "num_processed": len(data),
        }
        if len(data) > 0:
            response["output_rows"] = load(
                os.getenv("DATASET", "Facebook_dev"),
                ads_account_id,
                transform_add_batched_at(transform(data)),
            )
        return response

    return run
