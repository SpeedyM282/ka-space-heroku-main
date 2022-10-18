import json
import pprint
from datetime import datetime, date, timedelta
import logging
import math

from ka_space.celery import app
from api.helpers import Update_Daily
from mp.helpers import get_key, chunks, SLOW_TASK_TIMEOUT, bulk_insert_update

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import Transaction
    from mp_ozon.api import Api as ApiOzon, API_LIMIT_DAYS, TRANSACTION_FIELD_JSON
    from mp_ozon.errors import ErrorBadApiKey
except:
    logger.warning("MP_Ozon not available")

API_TRANSACTION_LIMIT_DAYS = 60  # если больше, то часть данных не возвращается
TRANSACTION_CHUNK_SIZE = 5000


@app.task
def update_transactions(*args, apikey_id=None, **kwargs):
    """Обновление товаров магазина

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
        result = api_transactions(apikey.shop, api, days=kwargs.get("days", 1))
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
    Update_Daily.transactions(params={"shop_id": apikey.shop.pk})

    return {"SUCCESS": f"{apikey.shop} {result}"}


def api_transactions(shop, api, days=1):
    start_at = datetime.now()

    total_rows = 0
    date_to = date(start_at.year, start_at.month + 1, 1) - timedelta(days=1)

    for period_offset in range(math.ceil(days / 30) + 1):
        date_from = date(date_to.year, date_to.month, 1)

        data = api.transactions(date_from=date_from, date_to=date_to)
        logger.info(
            f"{shop} Загружены транзакции: {len(data)} строк. Прошло {datetime.now() - start_at}"
        )
        if not len(data):
            logger.debug(
                f"Break on empty transactions response between {date_from} and {date_to}"
            )
            break
        for num, data_chunk in enumerate(chunks(data, TRANSACTION_CHUNK_SIZE)):
            transactions2db(data_chunk, shop)
        total_rows += len(data)

        # смещаем конечную дату диапазона и повторяем крайние Х дней
        date_to = date_from - timedelta(days=1)

    elapsed = datetime.now() - start_at
    msg = f"Транзакции обновлены: {total_rows} строк. Прошло {elapsed}"
    logger.info(f"{shop} {msg}")
    if elapsed.seconds > SLOW_TASK_TIMEOUT:
        logger.warning(f"{shop} Обновление транзакций шло {elapsed}")
    return msg


def transactions2db(data, shop):
    # bulk insert/update
    def transaction_changed(changed, obj, attr, value):
        # if getattr(obj, "posting_number") == "64037837-0077-3":
        #     print(attr, getattr(obj, attr), value, obj, obj.pk, obj.operation_type)
        skip = False
        if changed and attr == "services" and getattr(obj, "type") == "orders":
            obj_value = sorted(json.loads(getattr(obj, attr)), key=lambda d: d["name"])
            api_value = sorted(json.loads(value), key=lambda d: d["name"])
            changed = obj_value != api_value
            if changed and not api_value:
                # skip update on empty services
                skip = True
        return changed, skip

    msg = bulk_insert_update(
        data=data,
        key_fields=["operation_id"],
        changed_or_skip_func=transaction_changed,
        cls=Transaction,
        shop=shop,
    )
    logger.info(f"{shop} {msg}")
