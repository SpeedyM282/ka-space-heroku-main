import logging
from datetime import date, datetime
from io import BytesIO

from asgiref.sync import sync_to_async
from django.conf import settings
from django.http import HttpResponse
from django.templatetags.static import static
from rest_framework.authentication import TokenAuthentication, SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.authentication import JWTAuthentication
import httpx

from api.helpers import fetch_raw_sql
from mp.models import Shop
from ka_space.helpers import FileLogger

logger = logging.getLogger(__name__)

DEFAULT_CONVERT_DECIMAL = True
RETURN_MAX_ROWS = 9999
DAYS_DEFAULT = 180
TRANSACTION_DAYS_DEFAULT = 180

EMPTY_OFFER_ID = "lost_discounted"


class AnalyticsListView(APIView):
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """
        Возвращает данные аналитики для таблицы Статистика за X дней до Y даты

        Параметры GET:
        * shop_id - фильтр по магазину (по умолчанию отсутствуют)
        * days - кол-во дней статистики (по умолчанию 60)
        * before - дата, до которой выводить данные (по умолчанию текущая дата)
        * convert_values - конвертировать данные для Google.Sheets (заменяет . на ,)

        Наличие товара в справочнике необязательно, так как в аналитике присуствуют уцененные товары.

        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        # print(args, kwargs, "convert_values" in request.GET)
        shop_ids = get_shop_ids(request.user, request.GET)
        if not shop_ids:
            return Response({"Error": f"Shops not found."})
        page = int(request.GET.get("page", 0))
        limit = int(request.GET.get("limit", RETURN_MAX_ROWS))

        fields = [
            "ms.name as shop",
            f"CASE WHEN moso.offer_id IS NULL THEN '{EMPTY_OFFER_ID}' ELSE moso.offer_id END as offer_id",
            "CASE WHEN moso.type = 'discounted' and ad.stocks > 1 THEN 1 ELSE ad.stocks END as stocks",
            "moa.sku",
            "moa.date",
            "moa.session_view",
            "moa.session_view_pdp",
            "moa.session_view_search",
            "moa.hits_tocart",
            "moa.hits_tocart_pdp",
            "moa.hits_tocart_search",
            "moa.hits_view",
            "moa.hits_view_pdp",
            "moa.hits_view_search",
            "moa.returns",
            "moa.cancellations",
            "moa.delivered_units",
            "moa.ordered_units",
            "ROUND(AVG(moa.ordered_units) OVER w_sma7, 2) as ma7_ordered_units",
            "moa.revenue",
            "moa.adv_sum_all",
            "moa.adv_view_all",
            "moa.adv_view_pdp",
            "moa.adv_view_search_category",
            "moa.postings",
            "moa.postings_premium",
            "CASE WHEN ROW_NUMBER() OVER w_popular = 1 "
            "THEN -ROUND(AVG(moa.position_category) OVER w_sma3, 2) ELSE 0 END "
            "as position_category",
            "ad.selfbuy_cnt",
            "ad.selfbuy_amount",
            "-ad.premium as premium",
            "-ad.rassrochka as rassrochka",
            "CASE WHEN ROW_NUMBER() OVER w_popular = 1 THEN ad.adv_promo_bid/100 ELSE 0 END as adv_promo_bid",
            "CASE WHEN ROW_NUMBER() OVER w_popular = 1 THEN ad.adv_promo_visibility ELSE 0 END "
            "as adv_promo_visibility",
            "ROUND(AVG(CASE WHEN ordered_units = 0 THEN NULL ELSE revenue / ordered_units END) OVER w_sma30, 2) "
            "as ma30_avg_price",
            "moso.type",
        ]

        sql = f"""
        SELECT 
            {', '.join(fields)}
        FROM mp_ozon_analytics moa
        INNER JOIN mp_shop ms ON moa.shop_id = ms.id
        LEFT JOIN mp_ozon_sku_offer moso ON moa.sku = moso.sku
        LEFT JOIN api_daily ad ON moa.shop_id = ad.shop_id AND moa.date = ad.date AND moa.sku = ad.sku 
        WHERE moa.shop_id IN %(shop_ids)s AND moa.date BETWEEN (DATE(%(before)s) - INTERVAL %(days)s) AND %(before)s
        WINDOW w_popular AS (PARTITION BY moso.offer_id, moa.date ORDER BY moa.session_view DESC),
               w_sma30 AS (PARTITION BY moa.sku ORDER BY moa.date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW),       
               w_sma7 AS (PARTITION BY moa.sku ORDER BY moa.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
               w_sma3 AS (PARTITION BY moa.sku ORDER BY moa.date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        ORDER BY 
            moa.date DESC, 
            moso.offer_id ASC
        LIMIT {limit} OFFSET {page * limit}; 
        """
        rows = fetch_raw_sql(
            sql,
            {
                "shop_ids": shop_ids,
                "before": request.GET.get("before", date.today().strftime("%Y-%m-%d")),
                "days": f"{request.GET.get('days', DAYS_DEFAULT)} day",
            },
        )

        if DEFAULT_CONVERT_DECIMAL or "convert_values" in request.GET:
            rows = convert_float(
                rows,
                Analytics,
                fields=[
                    "ma7_ordered_units",
                    "selfbuy_amount",
                    "premium",
                    "rassrochka",
                    "adv_promo_bid",
                    "ma30_avg_price",
                ],
            )

        return Response(
            {
                "result": {
                    "shop": [str(s) for s in Shop.objects.filter(id__in=shop_ids)],
                    "page": page,
                    "count": len(rows),
                    "items": rows,
                }
            }
        )


class AdvertizingStatisticsListView(APIView):
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """
        Возвращает данные статистики по рекламным кампаниям

        Параметры GET:
        * shop_id - фильтр по магазину (по умолчанию отсутствуют)
        * days - кол-во дней статистики (по умолчанию 60)
        * before - дата, до которой выводить данные (по умолчанию текущая дата)
        * convert_values - конвертировать данные для Google.Sheets (заменяет . на ,)

        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        shop_ids = get_shop_ids(request.user, request.GET)
        if not shop_ids:
            return Response({"Error": f"Shops not found."})
        page = int(request.GET.get("page", 0))
        limit = int(request.GET.get("limit", RETURN_MAX_ROWS))

        fields = [
            f"CASE WHEN moso.offer_id IS NULL THEN '{EMPTY_OFFER_ID}' ELSE moso.offer_id END as offer_id",
            "moc.id as campaign_id",
            "moc.adv_type",
            "mos.page",
            "mos.dt as dt",
            "SUM(mos.views) as views",
            "SUM(mos.clicks) as clicks",
            "SUM(mos.expense) as expense",
            "SUM(mos.orders) as orders",
            "SUM(mos.revenue) as revenue",
            "(moc.state = 'CAMPAIGN_STATE_RUNNING')::int as is_active",
            "ms.name as shop",
        ]

        sql = f"""
        SELECT 
            {', '.join(fields)}
        FROM mp_ozon_statisticscampaignproduct mos
        LEFT JOIN mp_ozon_sku_offer moso ON mos.sku = moso.sku
        INNER JOIN mp_ozon_campaign moc ON moc.id = mos.campaign_id
        INNER JOIN mp_shop ms ON moc.shop_id = ms.id
        WHERE moc.shop_id IN %(shop_ids)s AND mos.dt BETWEEN (DATE(%(before)s) - INTERVAL %(days)s) 
            AND %(before)s
        GROUP BY moso.offer_id, mos.sku, moc.id, mos.page, mos.dt, ms.id 
        ORDER BY dt DESC, campaign_id, offer_id
        LIMIT {limit} OFFSET {page * limit}; 
        """
        rows = fetch_raw_sql(
            sql,
            {
                "shop_ids": shop_ids,
                "before": request.GET.get("before", date.today().strftime("%Y-%m-%d")),
                "days": f"{request.GET.get('days', DAYS_DEFAULT)} day",
            },
        )

        if DEFAULT_CONVERT_DECIMAL or "convert_values" in request.GET:
            rows = convert_float(rows, StatisticsCampaignProduct)

        return Response(
            {
                "result": {
                    "shop": [str(s) for s in Shop.objects.filter(id__in=shop_ids)],
                    "page": page,
                    "count": len(rows),
                    "items": rows,
                }
            }
        )


class TransactionsListView(APIView):
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """
        Возвращает список транзакций

        Параметры GET:
        * shop_id - фильтр по магазину (по умолчанию отсутствуют)
        * days - кол-во дней статистики (по умолчанию 60)
        * before - дата, до которой выводить данные (по умолчанию текущая дата)
        * convert_values - конвертировать данные для Google.Sheets (заменяет . на ,)

        Errors:
        Могут быть расхождения в суммах начислений, так как на свежих FBO-заказах бывает,
        что не возвращается fbo-сборка и магистраль. Например, 35674785-0102-2.

        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        shop_ids = get_shop_ids(request.user, request.GET)
        if not shop_ids:
            return Response({"Error": f"Shops not found."})
        page = int(request.GET.get("page", 0))
        limit = int(request.GET.get("limit", RETURN_MAX_ROWS))

        fields_01 = [
            "ms.name as shop",
            "mot.operation_type_name",
            "CASE WHEN mot.posting_number is not null THEN mot.posting_number ELSE mof.posting_number END "
            "as posting_number",
            "to_char(mof.created_at, 'YYYY-MM-DD HH24:MI:SS') as order_date",
            "to_char(mot.operation_date, 'YYYY-MM-DD HH24:MI:SS') as operation_date",
            "mof.status",
            "CASE WHEN mofp.sku is not null THEN mofp.sku ELSE mot.sku END as sku",
            "mop.offer_id",
            "CASE WHEN mofp.quantity is not null THEN mofp.quantity "
            "WHEN mot.type = 'compensation' THEN json_array_length(mot.items::json) END "
            "as quantity",
            "mofp.price",
            "mofp.quantity * mofp.commission_amount as comission",
            "CASE WHEN (mofp.item_services::json->>'marketplace_service_item_fulfillment')::numeric = 0 AND mot.sku_cnt = 1 "
            "THEN (mot.services_amount - (mofp.item_services::json->>'marketplace_service_item_deliv_to_customer')::numeric)  "
            "ELSE (mofp.item_services::json->>'marketplace_service_item_fulfillment')::numeric END "
            "as fulfillment",
            "(mofp.item_services::json->>'marketplace_service_item_direct_flow_trans')::numeric as direct_flow_trans",
            "(mofp.item_services::json->>'marketplace_service_item_deliv_to_customer')::numeric as deliv_to_customer",
            "CASE WHEN mot.sku_cnt > 1 AND mot.operation_type = 'OperationAgentDeliveredToCustomer' THEN "
            "mofp.quantity * mofp.price ELSE mot.accruals_for_sale END "
            "as calc_revenue",
            "CASE WHEN mot.sku_cnt > 1 AND mot.operation_type = 'OperationAgentDeliveredToCustomer' THEN "
            "mofp.quantity * mofp.price - mofp.quantity * mofp.commission_amount "
            "+ (mofp.item_services::json->>'marketplace_service_item_fulfillment')::numeric "
            "+ (mofp.item_services::json->>'marketplace_service_item_direct_flow_trans')::numeric "
            "+ (mofp.item_services::json->>'marketplace_service_item_deliv_to_customer')::numeric "
            "ELSE mot.amount END "
            "as calc_payment",
            "mot.sale_commission",
            "mot.services_amount",
            "mot.operation_type",
            "mot.type",
            "CASE WHEN mof.posting_number is null THEN mot.items::json->0->>'name' "
            "ELSE mot.operation_type_name END "
            "as basis",
            "(mof.analytics_data::json->>'delivery_type') as delivery_type",
            "(mof.analytics_data::json->>'warehouse_name') as warehouse",
            "(mof.analytics_data::json->>'region') as region",
            "(mof.analytics_data::json->>'city') as city",
            "mof.id as fbo_id",
            "mot.id as transaction_id",
            "CASE WHEN mof.created_at IS NOT NULL THEN mof.created_at ELSE operation_date END as sort_date",
            "1 as query",
        ]

        fields_02 = [
            "ms.name as shop",
            "null as operation_type_name",
            "mof.posting_number",
            "to_char(mof.created_at, 'YYYY-MM-DD HH24:MI:SS') as order_date",
            "null as operation_date",
            "mof.status",
            "mofp.sku",
            "mop.offer_id",
            "mofp.quantity",
            "mofp.price",
            "mofp.quantity * mofp.commission_amount as comission",
            "(mofp.item_services::json->>'marketplace_service_item_fulfillment')::numeric as fulfillment",
            "(mofp.item_services::json->>'marketplace_service_item_direct_flow_trans')::numeric as direct_flow_trans",
            "(mofp.item_services::json->>'marketplace_service_item_deliv_to_customer')::numeric as deliv_to_customer",
            "null as calc_revenue",
            "null as calc_payment",
            "null as sale_commission",
            "null as services_amount",
            "null as operation_type",
            "null as type",
            "null as basis",
            "null as delivery_type",
            "null as warehouse",
            "null as region",
            "null as city",
            "mof.id as fbo_id",
            "null as transaction_id",
            "mof.created_at as sort_date",
            "2 as query",
        ]

        sql = f"""
        (
        SELECT 
            {', '.join(fields_01)}
        FROM mp_ozon_transaction mot
        INNER JOIN mp_shop ms ON mot.shop_id = ms.id
        LEFT JOIN mp_ozon_fbo mof ON mot.shop_id = mof.shop_id AND mot.type = 'orders' AND mot.posting_number = mof.posting_number
        LEFT JOIN mp_ozon_fbo_product mofp ON mofp.order_id = mof.id
        LEFT JOIN mp_ozon_product mop ON mop.id = mofp.product_id OR (mot.sku > 0 AND mop.fbo_sku = mot.sku)
        WHERE mot.shop_id IN %(shop_ids)s 
            AND DATE(mot.operation_date) BETWEEN (DATE(%(before)s) - INTERVAL %(days)s) AND %(before)s        
        
        ) UNION (
        
        SELECT 
            {', '.join(fields_02)}
        FROM mp_ozon_fbo mof
        INNER JOIN mp_shop ms ON mof.shop_id = ms.id
        LEFT JOIN mp_ozon_fbo_product mofp ON mofp.order_id = mof.id
        LEFT JOIN mp_ozon_product mop ON mop.id = mofp.product_id
        WHERE 
            mof.posting_number not in (SELECT posting_number FROM mp_ozon_transaction mot WHERE posting_number is not null AND shop_id IN %(shop_ids)s) 
            AND mof.shop_id IN %(shop_ids)s 
            AND DATE(mof.created_at) BETWEEN (DATE(%(before)s) - INTERVAL %(days)s) AND %(before)s     
        )
        ORDER BY sort_date DESC
        LIMIT {limit} OFFSET {page * limit}; 
        """
        rows = fetch_raw_sql(
            sql,
            {
                "shop_ids": shop_ids,
                "before": request.GET.get("before", date.today().strftime("%Y-%m-%d")),
                "days": f"{request.GET.get('days', TRANSACTION_DAYS_DEFAULT)} day",
            },
        )

        if DEFAULT_CONVERT_DECIMAL or "convert_values" in request.GET:
            rows = convert_float(
                rows,
                Transaction,
                fields=[
                    "price",
                    "calc_accruals_for_sale",
                    "calc_amount",
                    "comission",
                    "fulfillment",
                    "direct_flow_trans",
                    "deliv_to_customer",
                    "calc_revenue",
                    "calc_payment",
                ],
            )

        return Response(
            {
                "result": {
                    "shop": [str(s) for s in Shop.objects.filter(id__in=shop_ids)],
                    "page": page,
                    "count": len(rows),
                    "items": rows,
                }
            }
        )


class ProfileView(APIView):
    authentication_classes = [TokenAuthentication, SessionAuthentication]
    permission_classes = [IsAuthenticated]

    def get(self, request, format=None):
        content = {
            "user": str(request.user),  # `django.contrib.auth.User` instance.
            "auth": str(request.auth),  # None
        }
        return Response(content)


class ProductsListView(APIView):
    authentication_classes = [
        TokenAuthentication,
        SessionAuthentication,
        JWTAuthentication,
    ]
    permission_classes = [IsAuthenticated]

    def get(self, request, *args, **kwargs):
        """
        Возвращает список товаров с полезными данными

        По умолчанию исклчаем товары со статусом: ARCHIVED

        Параметры GET:
        * shop_id - фильтр по магазину (по умолчанию отсутствуют)
        * convert_values - конвертировать данные для Google.Sheets (заменяет . на ,)

        :param request:
        :param args:
        :param kwargs:
        :return:
        """
        shop_ids = get_shop_ids(request.user, request.GET)
        if not shop_ids:
            return Response({"Error": f"Shops not found."})

        fields = [
            "mop.id",
            "mop.fbo_sku",
            "mop.offer_id",
            "price_index",
            "min_price",
            "marketing_price",
            "json_array_length(images::json) as images_cnt",
            "json_array_length(images360::json) as images360_cnt",
            "fbo.present as fbo_present",
            "fbo.reserved as fbo_reserved",
            "coalesce(mow_horug.for_sale, 0) as horug",
            "coalesce(mow_horug_bulky.for_sale, 0) as horug_bulky",
            "coalesce(mow_new_riga.for_sale, 0) as new_riga",
            "coalesce(mow_kzn.for_sale, 0) as kzn",
            "coalesce(mow_rnd.for_sale, 0) as rnd",
            "coalesce(mow_spb.for_sale, 0) as spb",
            "coalesce(mow_ekb.for_sale, 0) as ekb",
            "coalesce(mow_tvr.for_sale, 0) as tvr",
            "coalesce(mow_tvr_rfc.for_sale, 0) as tvr_rfc",
            "coalesce(mow_exp.for_sale, 0) as exp",
            "coalesce(mow_klg.for_sale, 0) as klg",
            "coalesce(mow_smr.for_sale, 0) as smr",
            "coalesce(mow_nsk.for_sale, 0) as nsk",
            "coalesce(mow_krr.for_sale, 0) as krr",
            "coalesce(mow_hbr.for_sale, 0) as hbr",
            "coalesce(mow.discounted_cnt, 0) as discounted_cnt",
            "fbs.present as fbs_present",
            "fbs.reserved as fbs_reserved",
            "mocp.campaign_cnt",
            "json_array_length(marketing_actions::json->'actions') as action_cnt",
            "(commissions::json->>'sales_percent')::float / 100 as commission",
            "volume_weight",
            "mop.shop_id",
            "ms.name as shop",
            "mop.name",
            "CASE WHEN mop.rich_content IS NOT NULL THEN true ELSE false END as rich_content",
            "mop.youtube",
            "mop.video_review",
            # comissions
            "marketing_price * (commissions::json->>'sales_percent')::float / 100 as commission_amount",
            # fbo
            "(commissions::json->>'fbo_fulfillment_amount')::float "
            "+ (commissions::json->>'fbo_direct_flow_trans_max_amount')::float "
            "+ (commissions::json->>'fbo_deliv_to_customer_amount')::float as fbo_amount",
            "(commissions::json->>'fbo_return_flow_amount')::float "
            "+ (commissions::json->>'fbo_return_flow_trans_max_amount')::float as fbo_return_amount",
            # fbs
            "(commissions::json->>'fbs_first_mile_max_amount')::float "
            "+ (commissions::json->>'fbs_direct_flow_trans_max_amount')::float "
            "+ (commissions::json->>'fbs_deliv_to_customer_amount')::float as fbs_amount",
            "(commissions::json->>'fbs_return_flow_amount')::float "
            "+ (commissions::json->>'fbs_return_flow_trans_max_amount')::float as fbs_return_amount",
            "mop.primary_image",
        ]

        sql = f"""
        SELECT 
            {', '.join(fields)}        
        FROM mp_ozon_product mop 
        INNER JOIN mp_shop ms ON mop.shop_id = ms.id
        LEFT JOIN mp_ozon_stock fbo ON 
                fbo.type = 'fbo' AND mop.id = fbo.product_id AND fbo."date" = (select max("date") from mp_ozon_stock)
        LEFT JOIN mp_ozon_stock fbs ON
                fbs.type = 'fbs' AND mop.id = fbs.product_id AND fbs."date" = fbo."date"

        LEFT JOIN mp_ozon_warehousestock mow_horug ON
                mow_horug.sku = mop.fbo_sku AND mow_horug."date" = fbo."date" 
                AND mow_horug.warehouse = 'horug' AND mow_horug.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_horug_bulky ON
                mow_horug_bulky.sku = mop.fbo_sku AND mow_horug_bulky."date" = fbo."date" 
                AND mow_horug_bulky.warehouse = 'horug_bulky' AND mow_horug_bulky.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_new_riga ON
                mow_new_riga.sku = mop.fbo_sku AND mow_new_riga."date" = fbo."date" 
                AND mow_new_riga.warehouse = 'new_riga' AND mow_new_riga.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_kzn ON
                mow_kzn.sku = mop.fbo_sku AND mow_kzn."date" = fbo."date" 
                AND mow_kzn.warehouse = 'kzn' AND mow_kzn.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_rnd ON
                mow_rnd.sku = mop.fbo_sku AND mow_rnd."date" = fbo."date" 
                AND mow_rnd.warehouse = 'rnd' and mow_rnd.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_spb ON
                mow_spb.sku = mop.fbo_sku AND mow_spb."date" = fbo."date" 
                AND mow_spb.warehouse = 'spb' AND mow_spb.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_ekb ON
                mow_ekb.sku = mop.fbo_sku AND mow_ekb."date" = fbo."date" 
                AND mow_ekb.warehouse = 'ekb' AND mow_ekb.discounted = false
                
        LEFT JOIN mp_ozon_warehousestock mow_exp ON
                mow_exp.sku = mop.fbo_sku AND mow_exp."date" = fbo."date" 
                AND mow_exp.warehouse = 'exp' AND mow_exp.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_klg ON
                mow_klg.sku = mop.fbo_sku AND mow_klg."date" = fbo."date" 
                AND mow_klg.warehouse = 'klg' AND mow_klg.discounted = false                
        LEFT JOIN mp_ozon_warehousestock mow_krr ON
                mow_krr.sku = mop.fbo_sku AND mow_krr."date" = fbo."date" 
                AND mow_krr.warehouse = 'krr' AND mow_krr.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_smr ON
                mow_smr.sku = mop.fbo_sku AND mow_smr."date" = fbo."date" 
                AND mow_smr.warehouse = 'smr' AND mow_smr.discounted = false
        
        LEFT JOIN mp_ozon_warehousestock mow_tvr ON
                mow_tvr.sku = mop.fbo_sku AND mow_tvr."date" = fbo."date" 
                AND mow_tvr.warehouse = 'tvr' AND mow_tvr.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_tvr_rfc ON
                mow_tvr_rfc.sku = mop.fbo_sku AND mow_tvr_rfc."date" = fbo."date" 
                AND mow_tvr_rfc.warehouse = 'tvr_rfc' AND mow_tvr_rfc.discounted = false        
        LEFT JOIN mp_ozon_warehousestock mow_nsk ON
                mow_nsk.sku = mop.fbo_sku AND mow_nsk."date" = fbo."date" 
                AND mow_nsk.warehouse = 'nsk' AND mow_nsk.discounted = false
        LEFT JOIN mp_ozon_warehousestock mow_hbr ON
                mow_hbr.sku = mop.fbo_sku AND mow_hbr."date" = fbo."date" 
                AND mow_hbr.warehouse = 'hbr' AND mow_nsk.discounted = false
        LEFT JOIN (
            SELECT
                offer_id,
                SUM(for_sale) as discounted_cnt,
                shop_id
            FROM mp_ozon_warehousestock
            WHERE date = (SELECT MAX(date) FROM mp_ozon_warehousestock)
            AND discounted=true
            GROUP BY offer_id, shop_id
        ) mow ON mop.offer_id = mow.offer_id AND mop.shop_id = mow.shop_id
        LEFT JOIN (
            SELECT 
                product_id, 
                COUNT(mocp.campaign_id) as campaign_cnt 
            FROM mp_ozon_campaignproduct mocp
            INNER JOIN mp_ozon_campaign moc ON moc.id = mocp.campaign_id
            WHERE moc.state = 'CAMPAIGN_STATE_RUNNING'
            GROUP BY product_id
        ) mocp ON mocp.product_id =  mop.id
        WHERE mop.shop_id IN %(shop_ids)s AND (mop.state NOT IN %(state_not)s OR fbo.present > 0)
        ORDER BY (fbo.present IS NOT NULL AND fbo.present > 0 AND mop.visible) DESC, ms.name, mop.offer_id
        """
        rows = fetch_raw_sql(
            sql,
            {
                "shop_ids": shop_ids,
                "state_not": tuple(
                    [
                        "ARCHIVED",
                    ]
                ),
            },
        )

        if "no_convert_decimal" not in request.GET:
            if DEFAULT_CONVERT_DECIMAL or "convert_values" in request.GET:
                rows = convert_float(
                    rows,
                    Product,
                    fields=[
                        "commission",
                        "commission_amount",
                        "fbo_amount",
                        "fbo_return_amount",
                        "fbs_amount",
                        "fbs_return_amount",
                    ],
                )

        return Response(
            {
                "result": {
                    "shop": [str(s) for s in Shop.objects.filter(id__in=shop_ids)],
                    "count": len(rows),
                    "items": rows,
                }
            }
        )


async def product_image(request, *args, **kwargs):
    params = {
        "offer_id": kwargs.get("offer_id"),
        "state": "MAIN",
    }
    if request.GET.get("s"):
        params["shop"] = request.GET.get("s")

    try:
        p = await sync_to_async(Product.objects.get, thread_sensitive=True)(**params)
        url = p.primary_image
    except Product.MultipleObjectsReturned:
        url = ""
    except Product.DoesNotExist:
        url = ""

    if "http" not in url:
        filelogger = FileLogger(settings.TMP_DIR / "primary_image_not_found.log")
        filelogger.append(f"{datetime.now()} \tParams: {params}")
        url = f"{request.scheme}://{request.get_host()}" + static("no-image.png")

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url)
        headers = r.headers
        img = BytesIO(r.content)

    # logger.info(
    #     f"API Image Request: {kwargs} Url: {url} Content-Type: {headers.get('content-type')}"
    # )

    return HttpResponse(img, content_type=headers.get("content-type"))


def get_shop_ids(user, get_data):
    """Возвращает ID доступных пользователю магазинов с учетом полученных условий

    * shop_id - уставляет перечисленные
    * exclude_shop_id - исключает перечисленные

    :param user:
    :param get_data:
    :return:
    """
    params = {"user": user, "is_active": True}
    exclude = {}
    if get_data.get("shop_id"):
        params["id__in"] = get_data.getlist("shop_id")
        if user.is_superuser:
            # remove user condition for superuser
            del params["user"]
    if get_data.get("exclude_shop_id"):
        exclude["id__in"] = get_data.getlist("exclude_shop_id")
    try:
        shop_ids = tuple(
            Shop.objects.filter(**params).exclude(**exclude).values_list(flat=True)
        )
    except Shop.DoesNotExist:
        return

    return shop_ids


def convert_float(rows, model, fields=[]):
    FIELD_FLOAT = [
        str(f).split(".")[-1]
        for f in model._meta.get_fields()
        if type(f).__name__ == "DecimalField"
    ] + fields

    for r in rows:
        for f in FIELD_FLOAT:
            if f in r:
                r[f] = 0 if f not in r or r[f] is None else str(r[f]).replace(".", ",")

    return rows
