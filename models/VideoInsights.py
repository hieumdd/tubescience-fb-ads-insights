from models.models import FacebookAdsInsights


class VideoInsights(FacebookAdsInsights):
    keys = {
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
    }

    fields = [
        "date_start",
        "date_stop",
        "account_id",
        "campaign_id",
        "adset_id",
        "ad_id",
        "spend",
    ]

    breakdowns = "video_asset"

    schema = [
        {"name": "account_id", "type": "NUMERIC"},
        {"name": "date_start", "type": "DATE"},
        {"name": "date_stop", "type": "DATE"},
        {"name": "campaign_id", "type": "NUMERIC"},
        {"name": "adset_id", "type": "NUMERIC"},
        {"name": "ad_id", "type": "NUMERIC"},
        {"name": "spend", "type": "NUMERIC"},
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
                "spend": row["spend"],
                "video_asset": {
                    "video_id": row["video_asset"].get("video_id"),
                    "url": row["video_asset"].get("url"),
                    "thumbnail_url": row["video_asset"].get("thumbnail_url"),
                    "video_name": row["video_asset"].get("video_name"),
                    "id": row["video_asset"].get("id"),
                }
                if row.get("video_asset", {})
                else {},
            }
            for row in rows
        ]
