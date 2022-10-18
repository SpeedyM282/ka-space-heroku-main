from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.urls import path, re_path, include
from django.views.decorators.cache import cache_page

from . import views as api_views

urlpatterns = [
    # api
    path(
        "analytics/",
        api_views.AnalyticsListView.as_view(),
    ),
    path("advertizing/", api_views.AdvertizingStatisticsListView.as_view()),
    path("profile/", api_views.ProfileView.as_view()),
    path("products/", api_views.ProductsListView.as_view()),
    path(
        "transactions/",
        api_views.TransactionsListView.as_view(),
    ),
    re_path(
        r"^img/(?P<offer_id>[\w\d\s\/\\.\\,\\*|\(\)+-]+)",
        api_views.product_image,
    ),
]
