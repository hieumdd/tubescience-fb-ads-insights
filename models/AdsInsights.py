from typing import Callable, TypedDict

from controller.facebook import ReportRunReq, request_async_report
from controller.bigquery import Loader, load


class FBAdsInsights(TypedDict):
    name: str
    request: ReportRunReq
    transform: Callable[[list[dict]], list[dict]]
    load: Loader


AdsInsights: FBAdsInsights = {
    "name": "AdsInsights",
    "request": request_async_report(
        fields=[
            "date_start",
            "date_stop",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "campaign_name",
            "adset_name",
            "ad_name",
            "clicks",
            "inline_link_clicks",
            "spend",
            "impressions",
            "actions",
            "action_values",
            "video_30_sec_watched_actions",
            "video_p25_watched_actions",
            "video_p50_watched_actions",
            "video_p75_watched_actions",
            "video_p95_watched_actions",
            "video_p100_watched_actions",
            "video_thruplay_watched_actions",
            "video_play_actions",
        ],
    ),
    "transform": lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "campaign_id": row["campaign_id"],
            "adset_id": row["adset_id"],
            "ad_id": row["ad_id"],
            "campaign_name": row["campaign_name"],
            "adset_name": row["adset_name"],
            "ad_name": row["ad_name"],
            "clicks": row.get("clicks"),
            "inline_link_clicks": row.get("inline_link_clicks"),
            "spend": row.get("spend"),
            "impressions": row.get("impressions"),
            "actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["actions"]
            ]
            if row.get("actions", [])
            else [],
            "action_values": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["action_values"]
            ]
            if row.get("action_values", [])
            else [],
            "video_30_sec_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_30_sec_watched_actions"]
            ]
            if row.get("video_30_sec_watched_actions", [])
            else [],
            "video_p25_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_p25_watched_actions"]
            ]
            if row.get("video_p25_watched_actions", [])
            else [],
            "video_p50_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_p50_watched_actions"]
            ]
            if row.get("video_p50_watched_actions", [])
            else [],
            "video_p75_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_p75_watched_actions"]
            ]
            if row.get("video_p75_watched_actions", [])
            else [],
            "video_p95_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_p95_watched_actions"]
            ]
            if row.get("video_p95_watched_actions", [])
            else [],
            "video_p100_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_p100_watched_actions"]
            ]
            if row.get("video_p100_watched_actions", [])
            else [],
            "video_play_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_play_actions"]
            ]
            if row.get("video_play_actions", [])
            else [],
            "video_thruplay_watched_actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["video_thruplay_watched_actions"]
            ]
            if row.get("video_thruplay_watched_actions", [])
            else [],
        }
        for row in rows
    ],
    "load": load(
        schema=[
            {"name": "account_id", "type": "NUMERIC"},
            {"name": "date_start", "type": "DATE"},
            {"name": "date_stop", "type": "DATE"},
            {"name": "campaign_id", "type": "NUMERIC"},
            {"name": "adset_id", "type": "NUMERIC"},
            {"name": "ad_id", "type": "NUMERIC"},
            {"name": "campaign_name", "type": "STRING"},
            {"name": "adset_name", "type": "STRING"},
            {"name": "ad_name", "type": "STRING"},
            {"name": "clicks", "type": "NUMERIC"},
            {"name": "inline_link_clicks", "type": "NUMERIC"},
            {"name": "spend", "type": "NUMERIC"},
            {"name": "impressions", "type": "NUMERIC"},
            {
                "name": "actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "action_values",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_30_sec_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_p25_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_p50_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_p75_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_p95_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_p100_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_play_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "video_thruplay_watched_actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {"name": "_batched_at", "type": "TIMESTAMP"},
        ],
        keys={
            "p_key": [
                "date_start",
                "date_stop",
                "account_id",
                "campaign_id",
                "adset_id",
                "ad_id",
            ],
            "incre_key": "_batched_at",
        },
    ),
}

VideoInsights: FBAdsInsights = {
    "name": "VideoInsights",
    "request": request_async_report(
        fields=[
            "date_start",
            "date_stop",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "campaign_name",
            "adset_name",
            "ad_name",
            "impressions",
            "clicks",
            "spend",
            "reach",
            "actions",
            "action_values",
        ],
        breakdowns="video_asset",
    ),
    "transform": lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "campaign_id": row["campaign_id"],
            "adset_id": row["adset_id"],
            "ad_id": row["ad_id"],
            "campaign_name": row["campaign_name"],
            "adset_name": row["adset_name"],
            "ad_name": row["ad_name"],
            "impressions": row.get("impressions"),
            "reach": row.get("reach"),
            "spend": row.get("spend"),
            "clicks": row.get("clicks"),
            "video_asset": {
                "video_id": row["video_asset"].get("video_id"),
                "url": row["video_asset"].get("url"),
                "thumbnail_url": row["video_asset"].get("thumbnail_url"),
                "video_name": row["video_asset"].get("video_name"),
                "id": row["video_asset"].get("id"),
            }
            if row.get("video_asset", {})
            else {},
            "actions": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["actions"]
            ]
            if row.get("actions", [])
            else [],
            "action_values": [
                {
                    "action_type": action.get("action_type"),
                    "value": action.get("value"),
                    "_1d_view": action.get("1d_view"),
                    "_1d_click": action.get("1d_click"),
                    "_7d_view": action.get("7d_view"),
                    "_7d_click": action.get("7d_click"),
                }
                for action in row["action_values"]
            ]
            if row.get("action_values", [])
            else [],
        }
        for row in rows
    ],
    "load": load(
        schema=[
            {"name": "account_id", "type": "NUMERIC"},
            {"name": "date_start", "type": "DATE"},
            {"name": "date_stop", "type": "DATE"},
            {"name": "campaign_id", "type": "NUMERIC"},
            {"name": "adset_id", "type": "NUMERIC"},
            {"name": "ad_id", "type": "NUMERIC"},
            {"name": "campaign_name", "type": "STRING"},
            {"name": "adset_name", "type": "STRING"},
            {"name": "ad_name", "type": "STRING"},
            {"name": "impressions", "type": "NUMERIC"},
            {"name": "clicks", "type": "NUMERIC"},
            {"name": "spend", "type": "NUMERIC"},
            {"name": "reach", "type": "NUMERIC"},
            {
                "name": "video_asset",
                "type": "RECORD",
                "fields": [
                    {"name": "video_id", "type": "INTEGER"},
                    {"name": "url", "type": "STRING"},
                    {"name": "thumbnail_url", "type": "STRING"},
                    {"name": "video_name", "type": "STRING"},
                    {"name": "id", "type": "INTEGER"},
                ],
            },
            {
                "name": "actions",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {
                "name": "action_values",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "action_type", "type": "STRING"},
                    {"name": "value", "type": "NUMERIC"},
                    {"name": "_1d_view", "type": "NUMERIC"},
                    {"name": "_1d_click", "type": "NUMERIC"},
                    {"name": "_7d_click", "type": "NUMERIC"},
                    {"name": "_7d_view", "type": "NUMERIC"},
                ],
            },
            {"name": "_batched_at", "type": "TIMESTAMP"},
        ],
        keys={
            "p_key": [
                "date_start",
                "date_stop",
                "account_id",
                "campaign_id",
                "adset_id",
                "ad_id",
                "video_asset.id",
            ],
            "incre_key": "_batched_at",
        },
    ),
}
