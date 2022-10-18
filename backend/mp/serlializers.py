from django.db.models import fields
from rest_framework import serializers
from .models import Shop, APIKey, Selfbuy


class ShopSerializer(serializers.ModelSerializer):
    class Meta:
        model = Shop
        fields = (
            "id",
            "shop_token",
            "name",
            "is_active",
            "user",
            "created_at",
            "updated_at",
        )


class APIKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = APIKey
        fields = (
            "id",
            "type",
            "name",
            "client_id",
            "client_secret",
            "is_active",
            "shop",
            "disabled_till",
            "created_at",
            "updated_at",
        )


class SelfbuySerializer(serializers.ModelSerializer):
    class Meta:
        model = Selfbuy
        fields = (
            "id",
            "order",
            "dt_buy",
            "dt_take",
            "offer_id",
            "name",
            "status",
            "shop",
            "created_at",
            "updated_at",
        )
