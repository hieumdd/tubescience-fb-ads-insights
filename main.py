from models.models import FacebookAdsInsights

# x = FacebookAdsInsights.factory("AdsInsights", "act_1747490262138666", None, None)
x = FacebookAdsInsights.factory("VideoAdsInsights", "act_1747490262138666", None, None)
y = x.run()
y

