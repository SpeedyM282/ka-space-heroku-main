from datetime import datetime, date, timedelta
from decimal import Decimal
import logging
import math

from django.db.utils import IntegrityError
from django.db.models import Q, Min
from django.db import transaction

from ka_space.celery import app
from api.helpers import execute_sql, Update_Daily
from mp.helpers import get_key, bulk_insert_update, chunks

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import FBO, FBO_Product, FBS, FBS_Product, Product, SKU_Offer
    from mp_ozon.api import Api as ApiOzon, API_LIMIT_DAYS
    from mp_ozon.errors import ErrorBadApiKey
    from mp_ozon.helpers import lost_product
except:
    logger.warning("MP_Ozon not available")

ORDER_CHUNK_SIZE = 3000


@app.task
def update_orders(*args, apikey_id=None, **kwargs):
    """Обновление FBO и FBS

    :param args:
    :param apikey:
    :param kwargs:
    :return:
    """
    if apikey_id is None:
        msg = f"Ошибка: Получен пустой ключ"
        logger.error(msg)
        return {"❌FAILED": msg}

    apikey = get_key(apikey_id)

    try:
        api = ApiOzon(apikey.client_id, apikey.client_secret, shop=apikey.shop)
        for model in [FBO, FBS]:
            api_orders(apikey.shop, api, days=kwargs.get("days", 1), model=model)
    except ErrorBadApiKey as ex:
        apikey.is_active = False
        apikey.save()
        msg = f"{ex} API-Ключ {apikey} выключен."
        logger.error(msg)
        return {"❌FAILED": msg}
    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        logger.exception(msg)
        return {"❌FAILED": msg}

    # обновление daily-статистики
    Update_Daily.orders(params={"shop_id": apikey.shop.pk})

    return {"result": f"{apikey.shop} Success"}


def api_orders(shop, api, days=1, model=None):
    start_at = datetime.now()

    # получаем минимальную дату незавершенного заказа
    q = model.objects.filter(Q(shop=shop) & ~Q(status__in=["delivered", "cancelled"]))
    check_min_date = q.aggregate(Min("created_at"))["created_at__min"]
    if check_min_date is None:
        # for empty table
        days = 360
    else:
        days_ago = (start_at.date() - check_min_date.date()).days
        days_ago = days_ago if days_ago < API_LIMIT_DAYS else API_LIMIT_DAYS
        days = max(days, days_ago)

    total_rows = 0
    date_to = date.today()
    days_max = days if days < API_LIMIT_DAYS else API_LIMIT_DAYS
    for period_offset in range(math.ceil(days / days_max)):
        date_from = date_to - timedelta(days=days_max)

        orders = api.fbo_fbs(dt=date_to, days=days_max, type_=model.__name__)
        logger.debug(
            f"{shop} {model.__name__} Загружены {date_to} ({days_max} дн) заказы: {len(orders)} строк. "
            f"Прошло {datetime.now() - start_at}"
        )
        if orders:
            for num, data_chunk in enumerate(chunks(orders, ORDER_CHUNK_SIZE)):
                orders2db(data_chunk, shop, model)
            total_rows += len(orders)
        else:
            # если данные отсутствуют, выходим
            break

        # смещаем конечную дату диапазона, смещение на 1 день игнорируем
        date_to = date_from

    if model.__name__ == "FBO":
        update_selfbuys(shop)
    logger.debug(
        f"{shop} {model.__name__} Заказы обновлены: {total_rows} строк. Прошло {datetime.now() - start_at}"
    )


def orders2db(orders, shop, model):
    """Обновляем заказы в БД

    :param orders:
    :param shop:
    :param model:
    :return:
    """
    if model.__name__ == "FBO":
        op_model = FBO_Product
    elif model.__name__ == "FBS":
        op_model = FBS_Product

    msg = bulk_insert_update(
        data=orders,
        key_fields=["order_id", "posting_number"],
        cls=model,
        shop=shop,
    )
    logger.info(f"{shop} {msg}")

    key_fields = ["order_id", "posting_number"]
    keys = {k: set(str(r[k]) for r in orders) for k in key_fields}
    filter_existing = {
        **{f"{k}__in": v for k, v in keys.items()},
        **({"shop": shop} if shop else {}),
    }
    with transaction.atomic():
        existing_objs = {
            tuple([str(getattr(obj, k)) for k in key_fields]): obj
            for obj in model.objects.filter(**filter_existing).select_for_update()
        }

    op_model_fields = [str(f).split(".")[-1] for f in op_model._meta.get_fields()]
    for o in orders:
        order = existing_objs[tuple([str(o[k]) for k in key_fields])]

        # add/update products
        for p in o["products"]:
            p["ozon_order_id"] = o["order_id"]
            product = get_product(
                {
                    "sku": p["sku"],
                    "offer_id": p["offer_id"],
                    "shop": shop,
                    "type": model.__name__,
                }
            )
            obj, created = op_model.objects.get_or_create(
                **{"order": order, "product": product}
            )

            changed_fields = []
            for attr, value in {
                k: v for k, v in p.items() if k in op_model_fields
            }.items():
                changed = getattr(obj, attr) != value
                if (
                    type(getattr(obj, attr)) in [date, datetime, int]
                    or type(value) == list
                ):
                    # check for date & int
                    changed = str(getattr(obj, attr)) != str(value)
                elif type(getattr(obj, attr)) == Decimal:
                    # check for decimal
                    changed = f"{getattr(obj, attr):.5f}" != f"{float(value):.5f}"
                if changed:
                    # print(
                    #     f"{attr}: {type(getattr(obj, attr))} {getattr(obj, attr)} => {type(value)} {value}"
                    # )
                    changed_fields.append(attr)
                    setattr(obj, attr, value)
            if changed_fields:
                obj.save()

        op_model.objects.filter(order=order).exclude(
            sku__in=[p["sku"] for p in o["products"]]
        ).delete()


def update_selfbuys(shop):
    """Обновляем самовыкупы из заказов

    FIXME Реализовано только для FBO

    :param shop:
    :return:
    """
    # обновляем самовыкупы
    sql = """
            UPDATE mp_selfbuy SET
                dt_buy = sub.in_process_at,
                dt_take = sub.operation_date,
                offer_id = sub.offer_id,
                name = sub.name,
                status = sub.status
            FROM (
                SELECT 
                    mof.order_number, 
                    mof.posting_number, 
                    mof.in_process_at, 
                    mot.operation_date, 
                    mofp.offer_id, 
                    mofp.name, 
                    mof.status,
                    mof.shop_id 
                FROM mp_ozon_fbo mof 
                INNER JOIN mp_ozon_fbo_product mofp ON mofp.order_id = mof.id
                LEFT JOIN mp_ozon_transaction mot ON mof.shop_id = mot.shop_id 
                    AND mot.posting_number = mof.posting_number
                WHERE mof.shop_id = %(shop_id)s
            ) sub
            WHERE dt_take IS NULL AND sub.shop_id = mp_selfbuy.shop_id 
                AND (sub.order_number = "order" OR sub.posting_number = "order")
            """
    execute_sql(
        sql,
        {
            "shop_id": shop.id,
        },
    )


def get_product(params=None):
    """Получаем товары магазина по SKU или Артикулу
    При поиске по артикулу добавляем условие типа.

    :param params:
    :return:
    """

    sku_offer_rows = SKU_Offer.objects.filter(
        Q(sku=params["sku"])
        | Q(offer_id=params["offer_id"], type=params["type"].lower()),
        product__shop=params["shop"],
    )
    if len(sku_offer_rows):
        product = sku_offer_rows[0].product
    else:
        product = None
        lost_product(params)

    return product
