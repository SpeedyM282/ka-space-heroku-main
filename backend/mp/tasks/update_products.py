from collections import ChainMap
from datetime import datetime
from decimal import Decimal
import logging
from pprint import pprint

from ka_space.celery import app
from mp.helpers import get_key, chunks

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import Product
    from mp_ozon.api import Api as ApiOzon
except:
    logger.warning("MP_Ozon not available")


@app.task
def update_products(*args, apikey_id=None, **kwargs):
    """Обновление товаров магазина

    ! Вызывается до обновления остатков из задачи update_stocks

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

    api = ApiOzon(apikey.client_id, apikey.client_secret, shop=apikey.shop)
    products = api.products()

    product_ids = [p["product_id"] for p in products]
    products_info = ChainMap(
        *[api.product(ids_chunk) for ids_chunk in chunks(product_ids, 1000)]
    )
    products_price = ChainMap(
        *[api.product_price(ids_chunk) for ids_chunk in chunks(product_ids, 1000)]
    )
    products_attribute = ChainMap(
        *[api.product_attribute(ids_chunk) for ids_chunk in chunks(product_ids, 1000)]
    )

    product_model_fields = [str(f).split(".")[-1] for f in Product._meta.get_fields()]
    total = 0
    for p in products:
        changed_fields = []
        product, created = Product.objects.get_or_create(**{"id": p["id"]})

        if created:
            product.shop = apikey.shop
        elif product.shop != apikey.shop:
            logger.error(
                f"Товар {product} принадлежит другому магазину: {product.shop}. "
                f"Меняем магазин на {apikey.shop}."
            )
            # выключаем другой магазин и привязываем продукт к текущему магазину
            if product.shop:
                product.shop.is_active = False
                product.shop.save()
            product.shop = apikey.shop
            logger.info(f"Теперь товар {product} принадлежит {product.shop}.")

        for attr, value in {
            k: v
            for k, v in {
                **products_price[p["product_id"]],
                **products_info[p["product_id"]],
                **products_attribute[p["product_id"]],
                **{"state": p["state"]},
            }.items()
            if k in product_model_fields
        }.items():
            changed = getattr(product, attr) != value
            if attr[-3:] == "_at":
                # convert to datetime
                changed = getattr(product, attr) != datetime.strptime(
                    value, "%Y-%m-%dT%H:%M:%S.%f%z"
                )
            elif type(getattr(product, attr)) == Decimal:
                # check for decimal
                value = 0 if value is None else value
                changed = f"{getattr(product, attr):.5f}" != f"{value:.5f}"
            if changed:
                # print(f"{attr}: {getattr(product, attr)} => {value}")
                changed_fields.append(attr)
                setattr(product, attr, value)
        if len(changed_fields):
            total += 1
            product.save()
            logger.debug(f"{product} Изменено: {changed_fields}")

    # удаление товаров
    to_delete = Product.objects.filter(shop=apikey.shop).exclude(
        id__in=[p["id"] for p in products]
    )
    if len(to_delete):
        to_delete.delete()

    msg = f"Обновление {total} товаров / Удалено {len(to_delete)} товаров"
    logger.info(f"{apikey.shop}: {msg}")
    return msg
