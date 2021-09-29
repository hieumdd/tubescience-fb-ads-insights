from models.models import FacebookAdsInsights


def main(request):
    data = request.get_json()
    print(data)

    if "ads_account_id" in data:
        response = FacebookAdsInsights.factory(
            "AdsInsights",
            data["ads_account_id"],
            data.get("start"),
            data.get("end"),
        ).run()

    print(response)
    return response
