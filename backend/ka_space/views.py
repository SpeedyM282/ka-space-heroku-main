import logging

from django.views import View
from django.shortcuts import render, redirect, HttpResponseRedirect, reverse
from django.contrib.auth import login
from django.contrib import messages

from rest_framework.authentication import TokenAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.authtoken.models import Token

from api.helpers import fetch_raw_sql
from ka_space.forms import RegistrationForm
from ka_space.helpers import Locking, ErrorIsLocked
from mp.models import Shop, APIKey

logger = logging.getLogger(__name__)


class ProfileView(View):
    template_name = "profile/index.html"
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        user = request.user
        token, created = Token.objects.get_or_create(user=user)

        shops_count = Shop.objects.filter(user=user).count()
        keys_result = APIKey.objects.filter(shop__user=user)
        keys_count = keys_result.count()
        keys = []
        for key in keys_result:
            lock = Locking(key.client_id)
            keys.append(f"{key.shop}/{key.name}:<br/> {lock.state().get('state')}")

        # ozon

        try:
            products_count = Product.objects.filter(shop__user=user).count()
            analytics_count = Analytics.objects.filter(shop__user=user).count()
            advertizing_count = StatisticsCampaignProduct.objects.filter(
                campaign__shop__user=user
            ).count()
            transactions_count = Transaction.objects.filter(shop__user=user).count()
        except:
            products_count = (
                analytics_count
            ) = advertizing_count = transactions_count = 0

        # locking_test = {}
        # lock = Locking("test", timeout=5, expire=15)
        # locking_test["state_before"] = lock.state()
        # locking_test["is_locked"] = lock.is_locked()
        # try:
        #     locking_test["acquired"] = lock.acquire()
        #     lock.set_state("Закрыли доступ")
        # except ErrorIsLocked:
        #     lock.set_state("Доступ закрыт")
        #     locking_test["released"] = lock.release()
        # locking_test["state"] = lock.state()

        return render(
            request,
            self.template_name,
            {
                "title": user,
                "user": user,
                "token": token.key,
                # "locking_test": locking_test,
                "keys_titles": "<br/>".join(keys),
                "statistics": {
                    "shops": shops_count,
                    "keys": keys_count,
                    "ozon_products": products_count,
                    "ozon_analytics": analytics_count,
                    "ozon_advertizing": advertizing_count,
                    "ozon_transactions": transactions_count,
                },
            },
        )

    def post(self, request, *args, **kwargs):
        action = request.POST.get("action")

        token, created = Token.objects.get_or_create(user=request.user)
        if action == "new_token" and not created:
            token.delete()
            token = Token.objects.create(user=request.user)

        return HttpResponseRedirect(reverse("profile"))


def register(request):
    if request.user.is_authenticated:
        messages.success(request, "Вы авторизованы.")
        return redirect("home")

    if request.method == "POST":
        form = RegistrationForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            messages.success(request, "Успешная регистрация.")
            return redirect("home")

        messages.error(
            request, f"Ошибка регистрации: {form.errors}", extra_tags="danger"
        )
    else:
        form = RegistrationForm()

    return render(
        request=request,
        template_name="registration/register.html",
        context={"title": "Регистрация", "register_form": form},
    )


class DashboardView(View):
    template_name = "administration/index.html"
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        user = request.user
        if not user.is_staff:
            return render(
                request,
                "administration/denied.html",
                {
                    "title": user,
                    "user": user,
                },
            )

        sql = """
        SELECT 
        au.username,
        ms.is_active,
        ms.shop_token, 
        ms.id,
        ms.name, 
        ma.client_ids as keys_titles,
        ma.cnt as keys,
        mop.cnt as products,
        moa.cnt as analytics,
        moc.cnt as campaigns,
        moscp.cnt as advert,
        mor.report_cnt as reports,
        mot.cnt as transactions,
        mofbo.cnt as fbo,
        mofbs.cnt as fbs
        FROM mp_shop ms
        INNER JOIN auth_user au ON au.id = ms.user_id
        LEFT JOIN (
            SELECT COUNT(*) as cnt, json_agg(client_id) as client_ids, shop_id 
            FROM mp_apikey 
            WHERE is_active and disabled_till < NOW()
            GROUP BY shop_id
        ) ma ON ma.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_product GROUP BY shop_id) mop ON mop.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_analytics GROUP BY shop_id) moa ON moa.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_campaign GROUP BY shop_id) moc ON moc.shop_id = ms.id
        LEFT JOIN (
            SELECT COUNT(mos.*) as cnt, moc.shop_id 
            FROM mp_ozon_statisticscampaignproduct mos, mp_ozon_campaign moc 
            WHERE mos.campaign_id = moc.id 
            GROUP BY shop_id
        ) moscp ON moscp.shop_id = ms.id
        LEFT JOIN (
            SELECT
                jsonb_object_agg(r.state, r.cnt) as report_cnt,
                r.shop_id
            FROM (
                SELECT state, COUNT(*) as cnt, shop_id 
                FROM mp_ozon_report 
                GROUP BY state, shop_id
            ) r
            GROUP BY r.shop_id
        ) mor ON mor.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_transaction GROUP BY shop_id) mot ON mot.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_fbo GROUP BY shop_id) mofbo ON mofbo.shop_id = ms.id
        LEFT JOIN (SELECT COUNT(*) as cnt, shop_id FROM mp_ozon_fbs GROUP BY shop_id) mofbs ON mofbs.shop_id = ms.id
        ORDER BY
        ms.is_active DESC, ms.id
        """
        rows = fetch_raw_sql(sql)
        shops = []
        for row in rows:
            keys = []
            if row["keys_titles"]:
                for key in row["keys_titles"]:
                    lock = Locking(key)
                    keys.append(
                        f"{key}:<br/> {lock.state().get('state')}".replace(
                            '"', "&quot;"
                        )
                    )
            shops.append(
                {
                    "user": row["username"],
                    "is_active": row["is_active"],
                    "shop": f"{row['shop_token'].upper()}.{row['id']}.{row['name']}",
                    "keys": row["keys"] if keys else "",
                    "keys_titles": "<br>".join(keys),
                    "products": row["products"],
                    "analytics": row["analytics"],
                    "campaigns": row["campaigns"],
                    "advertizing": row["advert"],
                    "report": row["reports"],
                    "transactions": row["transactions"],
                    "fbo": row["fbo"],
                    "fbs": row["fbs"],
                }
            )

        return render(
            request,
            self.template_name,
            {
                "title": "Admin Dashboard",
                "shops": shops,
            },
        )
