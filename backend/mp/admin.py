from django.contrib import admin
from .models import Shop, APIKey


class ShopAdmin(admin.ModelAdmin):
    list_display = (str, "user")


# Register your models here.
admin.site.register(Shop, ShopAdmin)
admin.site.register(APIKey)
