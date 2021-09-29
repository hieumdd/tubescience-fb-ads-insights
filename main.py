from models.models import FacebookAdsInsights
from tasks import create_tasks


def main(request):
    data = request.get_json()
    print(data)

    if "task" in data:
        response = create_tasks(data)
    elif "ads_account_id" in data:
        response = FacebookAdsInsights.factory(
            "AdsInsights",
            data["ads_account_id"],
            data.get("start"),
            data.get("end"),
        ).run()
    else:
        raise ValueError(data)

    print(response)
    return response
