from models.models import FacebookAdsInsights

class VideoAdsInsights(FacebookAdsInsights):
    fields = [
        "date_start",
        "date_stop",
        "campaign_id",
        "adset_id",
        "ad_id",
        "campaign_name",
        "adset_name",
        "ad_name",
        "video_30_sec_watched_actions",
        "video_p25_watched_actions",
        "video_p50_watched_actions",
        "video_p75_watched_actions",
        "video_p95_watched_actions",
        "video_p100_watched_actions",
        "video_play_actions",
    ]

    schema = [
        {"name": "account_id", "type": "NUMERIC"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "campaign_id", "type": "NUMERIC"},
        {"name": "adset_id", "type": "NUMERIC"},
        {"name": "ad_id", "type": "NUMERIC"},
        {"name": "campaign_name", "type": "STRING"},
        {"name": "adset_name", "type": "STRING"},
        {"name": "ad_name", "type": "STRING"},
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
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]

    def transform(self, rows):
        return [
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
            }
            for row in rows
        ]

