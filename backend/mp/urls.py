from django.contrib.auth.decorators import login_required
from django.urls import path, re_path, include

from . import views as mp_views

urlpatterns = [
    # shops
    path("shops", login_required(mp_views.ShopListView.as_view()), name="shops"),
    re_path(
        r"^shop/(?P<shop_id>\d+)/",
        login_required(mp_views.ShopFormView.as_view()),
        name="shop_form",
    ),
    re_path(
        r"^shop/delete/(?P<shop_id>\d+)/",
        login_required(mp_views.ShopFormView.delete),
        name="shop_delete",
    ),
    # APIKey
    path("apikey", login_required(mp_views.APIKeyListView.as_view()), name="apikeys"),
    re_path(
        r"^apikey/(?P<apikey_id>\d+)/",
        login_required(mp_views.APIKeyFormView.as_view()),
        name="apikey_form",
    ),
    re_path(
        r"^apikey/delete/(?P<apikey_id>\d+)/",
        login_required(mp_views.APIKeyFormView.delete),
        name="apikey_delete",
    ),
    # Selfbuy
    path(
        "selfbuy", login_required(mp_views.SelfbuyListView.as_view()), name="selfbuys"
    ),
    re_path(
        r"^selfbuy/(?P<selfbuy_id>\d+)/",
        login_required(mp_views.SelfbuyFormView.as_view()),
        name="selfbuy_form",
    ),
    re_path(
        r"^selfbuy/delete/(?P<selfbuy_id>\d+)/",
        login_required(mp_views.SelfbuyFormView.delete),
        name="selfbuy_delete",
    ),
]
