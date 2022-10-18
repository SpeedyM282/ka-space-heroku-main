from django.db import models

from mp.models import Shop


class Daily(models.Model):
    date = models.DateField(null=True)
    sku = models.IntegerField(null=True)

    stocks = models.IntegerField(default=0, null=True)

    selfbuy_cnt = models.IntegerField(default=0, null=True)
    selfbuy_amount = models.DecimalField(
        max_digits=20, decimal_places=4, default=0, null=True
    )

    premium = models.DecimalField(max_digits=20, decimal_places=4, default=0, null=True)
    rassrochka = models.DecimalField(
        max_digits=20, decimal_places=4, default=0, null=True
    )

    adv_promo_bid = models.DecimalField(
        max_digits=20, decimal_places=4, default=0, null=True
    )
    adv_promo_visibility = models.IntegerField(default=0, null=True)

    shop = models.ForeignKey(Shop, on_delete=models.CASCADE, null=True)

    class Meta:
        unique_together = ["shop", "date", "sku"]
        ordering = ["date", "sku"]

    def __str__(self):
        return f"{self.date} / {self.sku}  / {self.shop}"
