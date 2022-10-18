from django.views import View
from django.shortcuts import render, redirect, reverse, HttpResponseRedirect
from django.contrib import messages

from .models import Shop, APIKey, Selfbuy
from .forms import ShopForm, APIKeyForm, SelfbuyForm
from .tasks import (
    create_campaign_report,
    update_stocks,
    update_analytics,
    update_campaign_statistics,
    update_orders,
    update_transactions,
)

TITLES = {
    "shop": {
        "list": "Магазины",
        "update": "Изменение магазина",
        "update_success": "Магазин успешно сохранен",
        "not_found": "Магазин не найден",
        "delete_success": "Магазин удален",
        "delete_error": "Ошибка удаления магазина",
    },
    "apikey": {
        "list": "API-ключи",
        "update": "Изменение API-ключа",
        "update_success": "API-ключ успешно сохранен",
        "not_found": "API-ключ не найден",
        "delete_success": "API-ключ удален",
        "delete_error": "Ошибка удаления API-ключа",
        "task_started": "Создана задача обновления данных",
    },
    "selfbuy": {
        "list": "Самовыкупы",
        "update": "Изменение самовыкупа",
        "update_success": "Самовыкуп успешно сохранен",
        "not_found": "Самовыкуп не найден",
        "delete_success": "Самовыкуп удален",
        "delete_error": "Ошибка удаления самовыкупа",
    },
}


class ShopListView(View):
    template_name = "shop/list.html"
    context_object_name = "list"

    def get(self, request, *args, **kwargs):
        items = Shop.objects.filter(user=request.user)
        return render(
            request,
            self.template_name,
            {"title": TITLES["shop"]["list"], "list": items},
        )


class ShopFormView(View):
    template_name = "shop/form.html"

    def get(self, request, *args, **kwargs):
        shop_id = kwargs.get("shop_id", 0)

        try:
            shop = Shop.objects.get(user=request.user, id=shop_id)
        except Shop.DoesNotExist as ex:
            if int(shop_id) > 0:
                messages.add_message(
                    request,
                    messages.ERROR,
                    TITLES["shop"]["not_found"],
                    extra_tags="danger",
                )
                return HttpResponseRedirect(reverse("shops"))
            shop = Shop()

        form = ShopForm(instance=shop)

        return render(
            request,
            self.template_name,
            {"title": TITLES["shop"]["update"], "obj": shop, "form": form},
        )

    def post(self, request, *args, **kwargs):
        shop_id = kwargs.get("shop_id", 0)

        try:
            shop = Shop.objects.get(user=request.user, id=shop_id)
        except Shop.DoesNotExist as ex:
            shop = Shop(user=request.user)

        form = ShopForm(request.POST or None, instance=shop)

        if form.is_valid():
            messages.add_message(
                request,
                messages.INFO,
                TITLES["shop"]["update_success"],
                extra_tags="success",
            )
            form.save()
            return HttpResponseRedirect(reverse("shops"))
        else:
            messages.error(
                request,
                f"Форма содержит ошибки: {form.errors}",
                extra_tags="danger",
            )
            return render(
                request,
                self.template_name,
                {"title": TITLES["shop"]["update"], "obj": shop, "form": form},
            )

    @staticmethod
    def delete(request, *args, **kwargs):
        shop_id = kwargs.get("shop_id", 0)
        try:
            shop = Shop.objects.get(user=request.user, id=shop_id)
            shop.delete()
            messages.add_message(
                request,
                messages.INFO,
                TITLES["shop"]["delete_success"],
                extra_tags="info",
            )
        except Shop.DoesNotExist as ex:
            messages.add_message(
                request,
                messages.ERROR,
                TITLES["shop"]["not_found"],
                extra_tags="danger",
            )

        return HttpResponseRedirect(reverse("shops"))


class APIKeyListView(View):
    template_name = "api_key/list.html"
    context_object_name = "list"

    def get(self, request, *args, **kwargs):
        items = APIKey.objects.filter(shop__user=request.user)
        return render(
            request,
            self.template_name,
            {
                "title": TITLES["apikey"]["list"],
                "list": items,
            },
        )


class APIKeyFormView(View):
    template_name = "api_key/form.html"

    def get(self, request, *args, **kwargs):
        apikey_id = kwargs.get("apikey_id", 0)

        try:
            apikey = APIKey.objects.get(id=apikey_id, shop__user=request.user)
        except APIKey.DoesNotExist as ex:
            if int(apikey_id) > 0:
                messages.add_message(
                    request,
                    messages.ERROR,
                    TITLES["apikey"]["not_found"],
                    extra_tags="danger",
                )
                return HttpResponseRedirect(reverse("apikeys"))
            apikey = APIKey()

        form = APIKeyForm(instance=apikey, user=request.user)

        return render(
            request,
            self.template_name,
            {"title": TITLES["apikey"]["update"], "obj": apikey, "form": form},
        )

    def post(self, request, *args, **kwargs):
        is_new = False
        apikey_id = kwargs.get("apikey_id", 0)

        try:
            apikey = APIKey.objects.get(id=apikey_id, shop__user=request.user)
        except APIKey.DoesNotExist:
            is_new = True
            apikey = APIKey()

        form = APIKeyForm(request.POST or None, instance=apikey)

        if form.is_valid():
            messages.add_message(
                request,
                messages.INFO,
                TITLES["apikey"]["update_success"],
                extra_tags="success",
            )
            apikey = form.save()

            if is_new and apikey.pk and apikey.type in ["ozon", "performance"]:
                # Запускаем задачи для первого обновления
                messages.add_message(
                    request,
                    messages.INFO,
                    TITLES["apikey"]["task_started"],
                    extra_tags="success",
                )
                if apikey.type == "ozon":
                    update_stocks.delay(apikey_id=apikey.pk, shop_id=apikey.shop_id)
                    update_analytics.s(
                        apikey_id=apikey.pk, days=180, shop_id=apikey.shop_id
                    ).apply_async(
                        countdown=60
                    )  # через минуту

                    update_transactions.s(
                        apikey_id=apikey.pk, days=180, shop_id=apikey.shop_id
                    ).apply_async(
                        countdown=120
                    )  # через две минуты

                    update_orders.s(
                        apikey_id=apikey.pk, days=180, shop_id=apikey.shop_id
                    ).apply_async(
                        countdown=180
                    )  # через три минуты

                elif apikey.type == "performance":
                    update_campaign_statistics.s(
                        apikey_id=apikey.pk, days=180, shop_id=apikey.shop_id
                    ).apply_async(
                        countdown=60
                    )  # через 1 минуты обновляем кампании

                    create_campaign_report.s(
                        apikey_id=apikey.pk, days=60, shop_id=apikey.shop_id
                    ).apply_async(
                        countdown=120
                    )  # через 2 минуты создаем отчеты

            return HttpResponseRedirect(reverse("apikeys"))
        else:
            messages.error(
                request,
                f"Форма содержит ошибки: {form.errors}",
                extra_tags="danger",
            )
            return render(
                request,
                self.template_name,
                {"title": TITLES["apikey"]["update"], "obj": apikey, "form": form},
            )

    @staticmethod
    def delete(request, *args, **kwargs):
        apikey_id = kwargs.get("apikey_id", 0)
        try:
            apikey = APIKey.objects.get(id=apikey_id, shop__user=request.user)
            apikey.delete()
            messages.add_message(
                request,
                messages.INFO,
                TITLES["apikey"]["delete_success"],
                extra_tags="info",
            )
        except APIKey.DoesNotExist as ex:
            messages.add_message(
                request,
                messages.ERROR,
                TITLES["apikey"]["not_found"],
                extra_tags="danger",
            )

        return HttpResponseRedirect(reverse("apikeys"))


class SelfbuyListView(View):
    template_name = "selfbuy/list.html"
    context_object_name = "list"

    def get(self, request, *args, **kwargs):
        items = Selfbuy.objects.filter(shop__user=request.user)
        return render(
            request,
            self.template_name,
            {"title": TITLES["selfbuy"]["list"], "list": items},
        )


class SelfbuyFormView(View):
    template_name = "selfbuy/form.html"

    def get(self, request, *args, **kwargs):
        selfbuy_id = kwargs.get("selfbuy_id", 0)

        try:
            selfbuy = Selfbuy.objects.get(shop__user=request.user, id=selfbuy_id)
        except Selfbuy.DoesNotExist as ex:
            if int(selfbuy_id) > 0:
                messages.add_message(
                    request,
                    messages.ERROR,
                    TITLES["selfbuy"]["not_found"],
                    extra_tags="danger",
                )
                return HttpResponseRedirect(reverse("selfbuys"))
            selfbuy = Selfbuy()

        form = SelfbuyForm(instance=selfbuy, user=request.user)

        return render(
            request,
            self.template_name,
            {"title": TITLES["selfbuy"]["update"], "obj": selfbuy, "form": form},
        )

    def post(self, request, *args, **kwargs):
        selfbuy_id = kwargs.get("selfbuy_id", 0)

        try:
            selfbuy = Selfbuy.objects.get(shop__user=request.user, id=selfbuy_id)
        except Selfbuy.DoesNotExist as ex:
            selfbuy = Selfbuy()

        form = SelfbuyForm(request.POST or None, instance=selfbuy)

        if form.is_valid():
            messages.add_message(
                request,
                messages.INFO,
                TITLES["selfbuy"]["update_success"],
                extra_tags="success",
            )
            form.save()

            return HttpResponseRedirect(reverse("selfbuys"))
        else:
            messages.error(
                request,
                f"Форма содержит ошибки: {form.errors}",
                extra_tags="danger",
            )
            return render(
                request,
                self.template_name,
                {"title": TITLES["selfbuy"]["update"], "obj": selfbuy, "form": form},
            )

    @staticmethod
    def delete(request, *args, **kwargs):
        selfbuy_id = kwargs.get("selfbuy_id", 0)
        try:
            selfbuy = Selfbuy.objects.get(shop__user=request.user, id=selfbuy_id)
            selfbuy.delete()
            messages.add_message(
                request,
                messages.INFO,
                TITLES["selfbuy"]["delete_success"],
                extra_tags="info",
            )
        except Shop.DoesNotExist as ex:
            messages.add_message(
                request,
                messages.ERROR,
                TITLES["selfbuy"]["not_found"],
                extra_tags="danger",
            )

        return HttpResponseRedirect(reverse("selfbuys"))
