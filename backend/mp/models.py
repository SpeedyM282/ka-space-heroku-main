from django.db import models
from django.utils import timezone

from ka_space.user.models import User


class Shop(models.Model):
    SHOP_TOKENS = (
        ("ozon", "Ozon"),
        ("wb", "Wildberries"),
    )

    shop_token = models.CharField(max_length=10, choices=SHOP_TOKENS, db_index=True)
    name = models.CharField(max_length=200)
    is_active = models.BooleanField(default=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-is_active", "id"]

    def __str__(self):
        return f"{'✅' if self.is_active else '⭕'} {self.shop_token.upper()}.{self.pk}.{self.name}"

    def save(self, *args, **kwargs):
        if self.pk is not None:
            self.updated_at = timezone.now()

        super().save(*args, **kwargs)


class APIKey(models.Model):
    """
    Модель для хранения API ключей для доступа к рекламе на Озоне
    """

    TYPES = (
        ("ozon", "Ozon API"),
        ("performance", "Ozon Performance API"),
        ("wb_x32", "WB x32 API"),
        ("wb_x64", "WB x64 API"),
    )

    type = models.CharField(max_length=16, choices=TYPES, db_index=True)
    name = models.CharField(max_length=200)
    client_id = models.CharField(max_length=128, db_index=True)
    client_secret = models.CharField(max_length=256, db_index=True)
    is_active = models.BooleanField(default=False)
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, null=True)
    disabled_till = models.DateTimeField(auto_now_add=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)

    @property
    def is_disabled(self):
        return self.disabled_till > timezone.now()

    class Meta:
        unique_together = (("shop", "client_id"),)
        ordering = ["-is_active", "shop", "type", "name"]

    def __str__(self):
        return f"{'✅' if self.is_active else '⭕'} {self.type}.{self.name} / {self.client_id}"

    def save(self, *args, **kwargs):
        if self.pk is not None:
            self.updated_at = timezone.now()

        super().save(*args, **kwargs)


class Selfbuy(models.Model):
    """
    Модель для самовыкупов, храним только номера заказов, другие данные получаем из транщакций или ФБО
    """

    order = models.CharField(max_length=128)
    dt_buy = models.DateField(null=True)
    dt_take = models.DateField(null=True)
    offer_id = models.CharField(max_length=64, null=True)
    name = models.CharField(max_length=256, null=True)
    status = models.CharField(max_length=64, null=True)
    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = (("shop", "order"),)
        ordering = ["dt_buy", "order"]

    def __str__(self):
        return f"{self.dt_buy} / {self.order}"

    def save(self, *args, **kwargs):
        if self.pk is not None:
            self.updated_at = timezone.now()

        super().save(*args, **kwargs)
