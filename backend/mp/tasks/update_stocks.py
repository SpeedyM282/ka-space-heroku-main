from decimal import Decimal
import logging

from django.db import connection
from django.db.utils import IntegrityError

from ka_space.celery import app
from api.helpers import execute_sql, Update_Daily
from mp.helpers import get_key
from . import update_products

logger = logging.getLogger(__name__)


try:
    from mp_ozon.models import Product, Stock, WarehouseStock
    from mp_ozon.api import Api as ApiOzon
except:
    logger.warning("MP_Ozon not available")


@app.task
def update_stocks(*args, apikey_id=None, **kwargs):
    """Обновление складских остатков

    :param args:
    :param apikey:
    :param kwargs:
    :return:
    """
    if apikey_id is None:
        msg = f"Ошибка: Получен пустой ключ"
        logger.error(msg)
        return {"❌FAILED": msg}

    # обновление товаров в текущей задаче, синхронно, не откладывая
    result_products = update_products(apikey_id=apikey_id)

    apikey = get_key(apikey_id)

    cnt = Product.objects.filter(shop=apikey.shop).count()
    result_stocks = ""
    if cnt:
        api = ApiOzon(apikey.client_id, apikey.client_secret, shop=apikey.shop)
        try:
            result_stocks = api_stocks(apikey.shop, api)
        except Exception as ex:
            result_stocks = "Ошибка обновления остатков"
            logger.exception(ex)

        try:
            result_wh_stocks = api_warehouse_stocks(apikey.shop, api)
        except Exception as ex:
            result_wh_stocks = "Ошибка обновления остатков на кластерах"
            logger.exception(ex)
    else:
        logger.error(
            f"{apikey.shop} Отсутствуют товары в магазине. Обновление остатков остановлено."
        )

    # обновление daily-статистики
    Update_Daily.stocks(params={"shop_id": apikey.shop.pk})

    # заполнение таблицы SKU х Артикул для корректного сопоставления товаров
    update_sku_offer(apikey.shop)

    return {
        "SUCCESS": f"{apikey.shop} {result_products} / {result_stocks} / {result_wh_stocks}"
    }


def api_stocks(shop, api):
    stocks = api.stocks()

    model_fields = [str(f).split(".")[-1] for f in Stock._meta.get_fields()]
    stock_model_key = ["product_id", "date", "type"]
    total = 0
    for s in stocks:
        try:
            key = {
                **{"shop": shop},
                **{k: v for k, v in s.items() if k in stock_model_key},
            }
            obj, created = Stock.objects.get_or_create(**key)
        except IntegrityError as ex:
            logger.error(f"{shop} Ошибка обновления остатков: {ex} {s}")
            continue

        changed_fields = []
        for attr, value in {k: v for k, v in s.items() if k in model_fields}.items():
            changed = getattr(obj, attr) != value
            if type(getattr(obj, attr)) == Decimal:
                # check for decimal
                value = 0 if value is None else value
                changed = f"{getattr(obj, attr):.5f}" != f"{value:.5f}"
            if changed:
                # print(f"{attr}: {getattr(obj, attr)} => {value}")
                changed_fields.append(attr)
                setattr(obj, attr, value)
        # stock.present = s["present"]
        # stock.reserved = s["reserved"]
        if changed_fields:
            total += 1
            obj.save()
            logger.debug(f"Изменен остаток: {obj} {changed_fields}")

    msg = f"Обновлено {total} строк складских остатков"
    logger.info(f"{shop}: {msg}")
    return msg


def api_warehouse_stocks(shop, api):
    total = 0
    stocks = api.warehouse_stocks()
    total += warehouse_stocks_to_db(stocks, shop)

    msg = f"Обновление {total} строк остатков на кластерах"
    logger.info(f"{shop}: {msg}")
    return msg


def warehouse_stocks_to_db(stocks, shop):
    model_fields = [str(f).split(".")[-1] for f in WarehouseStock._meta.get_fields()]
    stock_model_key = ["sku", "date", "warehouse"]
    total = 0
    for s in stocks:
        obj, created = WarehouseStock.objects.get_or_create(
            **{
                **{"shop": shop},
                **{k: v for k, v in s.items() if k in stock_model_key},
            }
        )

        changed_fields = []
        for attr, value in {k: v for k, v in s.items() if k in model_fields}.items():
            changed = getattr(obj, attr) != value
            if type(getattr(obj, attr)) == Decimal:
                # check for decimal
                value = 0 if value is None else value
                changed = f"{getattr(obj, attr):.5f}" != f"{value:.5f}"
            if changed:
                # print(f"{attr}: {getattr(obj, attr)} => {value}")
                changed_fields.append(attr)
                setattr(obj, attr, value)
        if changed_fields:
            total += 1
            obj.save()
            logger.debug(f"Изменен остаток: {obj} Поля: {changed_fields}")

    return total


def update_sku_offer(shop):
    """Обновление артикулов товаров

    :param shop:
    :return:
    """
    sql = """
        INSERT INTO mp_ozon_sku_offer  
        (sku, type, offer_id, product_id)
        (
            SELECT 
            DISTINCT mow.sku, 'discounted' AS type, mow.offer_id, mop.id AS product_id
            FROM mp_ozon_warehousestock mow 
            INNER JOIN mp_ozon_product mop ON mow.offer_id = mop.offer_id
            WHERE mow.discounted = true AND mow.shop_id = %(shop_id)s
            ORDER BY mow.offer_id
        ) ON CONFLICT (sku, product_id) DO NOTHING;
    """
    execute_sql(
        sql,
        {
            "shop_id": shop.pk,
        },
    )
    logger.debug(f"{shop}: Обновлены SKU и артикулы уцененных товаров")

    sql = """
        INSERT INTO mp_ozon_sku_offer  
        (sku, type, offer_id, product_id)
        (
            SELECT 
            DISTINCT fbo_sku, 'fbo' as type, mop.offer_id, mop.id as product_id
            FROM mp_ozon_product mop 
            WHERE mop.shop_id = %(shop_id)s
            ORDER BY mop.offer_id
        ) ON CONFLICT (sku, product_id) DO UPDATE SET 
            type = excluded.type;
    """
    execute_sql(
        sql,
        {
            "shop_id": shop.pk,
        },
    )
    logger.debug(f"{shop}: Обновлены SKU и артикулы FBO товаров")

    sql = """
        INSERT INTO mp_ozon_sku_offer  
        (sku, type, offer_id, product_id)
        (
            SELECT 
            DISTINCT fbs_sku, 'fbs' as type, mop.offer_id, mop.id as product_id
            FROM mp_ozon_product mop 
            WHERE mop.shop_id = %(shop_id)s
            ORDER BY mop.offer_id
        ) ON CONFLICT (sku, product_id) DO UPDATE SET 
            type = excluded.type;
    """
    execute_sql(
        sql,
        {
            "shop_id": shop.pk,
        },
    )
    logger.debug(f"{shop}: Обновлены SKU и артикулы FBS товаров")

    sql = """
        UPDATE mp_ozon_sku_offer moso SET
        offer_id = mop.offer_id
        FROM mp_ozon_product mop
        WHERE mop.id = moso.product_id AND mop.shop_id = %(shop_id)s AND moso.offer_id != mop.offer_id;
    """
    execute_sql(
        sql,
        {
            "shop_id": shop.pk,
        },
    )
    logger.debug(f"{shop}: Обновлены артикулы товаров")

    # Remove Lost Products
    sql = """
        DELETE FROM mp_ozon_productlost WHERE sku IN (SELECT sku FROM mp_ozon_sku_offer WHERE shop_id = %(shop_id)s);
    """
    execute_sql(
        sql,
        {
            "shop_id": shop.pk,
        },
    )
    logger.debug(f"{shop}: Удалены товары, отсутствующие на Озоне")

    return
