from datetime import datetime, date, timedelta
from decimal import Decimal
import logging
import math
from pprint import pprint
import difflib

from django.db.utils import IntegrityError
from django.db.models import Q

from ka_space.celery import app
from mp.helpers import get_key, SLOW_TASK_TIMEOUT, bulk_insert_update, chunks

logger = logging.getLogger(__name__)


try:
    from mp_ozon.models import Product, SKU_Offer, Analytics
    from mp_ozon.api import Api as ApiOzon, API_LIMIT_DAYS, METRICS, API_LIMIT_METRICS
    from mp_ozon.errors import ErrorRateLimit, ErrorRequest, ErrorBadApiKey
except:
    logger.warning("MP_Ozon not available")


@app.task(bind=True)
def update_analytics(self, *args, apikey_id=None, **kwargs):
    """Обновление аналитики магазина

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
        api_analytics(
            apikey.shop,
            api,
            days=kwargs.get("days", 1),
            days_step=kwargs.get("days_step"),
            task=self,
        )
    except ErrorRateLimit as ex:
        return {"❌FAILED": f"{ex}"}
    except ErrorBadApiKey as ex:
        apikey.is_active = False
        apikey.save()
        msg = f"{ex} API-Ключ {apikey} выключен."
        logger.error(msg)
        return {"❌FAILED": msg}
    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        if "500" not in msg:
            logger.exception(msg)
        return {"❌FAILED": msg}

    return {"SUCCESS": f"{apikey.shop}"}


def api_analytics(shop, api, days=1, days_step=None, task=None):
    """

    :param shop:
    :param api:
    :param days: всего дней
    :param days_step: дней за раз
    :param task:
    :return:
    """
    start_at = datetime.now()

    days_limit = (
        days_step
        if days_step is not None and days_step < API_LIMIT_DAYS
        else API_LIMIT_DAYS
    )
    days_max = days if days < days_limit else days_limit

    total_rows = 0
    total_periods = math.ceil(days / days_max)
    for num, metrics in enumerate(chunks(METRICS, API_LIMIT_METRICS)):
        date_to = date.today()
        for period_offset in range(total_periods):
            date_from = date_to - timedelta(days=days_max)

            # обновляем статус задачи
            # task.send_event(f"task-download {period_offset/total_periods * 100.:.0f}%")

            try:
                data = api.analytics(dt=date_to, days=days_max, metrics=metrics)
                logger.info(
                    f"{shop} Загружена аналитика: {len(data)} строк. Прошло {datetime.now() - start_at}"
                )
            except ErrorRequest as ex:
                logger.error(ex)
                data = []

            if len(data):
                analytics2db(data, shop)
                total_rows += len(data)

            # смещаем конечную дату диапазона, смещение на 1 день игнорируем
            date_to = date_from

    elapsed = datetime.now() - start_at
    logger.info(f"{shop} Аналитика. Обновлено {total_rows} строк. Прошло {elapsed}")
    if elapsed.seconds > SLOW_TASK_TIMEOUT:
        logger.warning(f"{shop} Обновление аналитики шло {elapsed}")


def analytics2db(data, shop):
    """Обновляем аналитику в БД

    :param data:
    :param shop:
    :return:
    """
    msg = bulk_insert_update(
        data=data,
        key_fields=["date", "sku"],
        cls=Analytics,
        shop=shop,
    )
    logger.info(f"{shop} {msg}")
