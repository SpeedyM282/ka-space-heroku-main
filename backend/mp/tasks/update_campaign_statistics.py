from datetime import datetime
import logging

from django.db.utils import IntegrityError
from django.db.models import Q

from ka_space.celery import app
from mp.helpers import get_key, bulk_insert_update
from . import update_campaigns

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import Product, CampaignProduct, Campaign, StatisticsCampaign
    from mp_ozon.api import ApiPerformance
    from mp_ozon.errors import ErrorRequest404, ErrorLocked
except:
    logger.warning("MP_Ozon not available")


@app.task
def update_campaign_statistics(*args, apikey_id=None, **kwargs):
    """Обновление статистики рекламных кампаний магазина

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
    if not Product.objects.filter(shop=apikey.shop).count():
        return {"❌FAILED": f"{apikey.shop}: Товары не найдены"}

    # обновление рекламных кампаний в текущей задаче, синхронно, не откладывая
    update_campaigns(apikey_id=apikey_id)

    try:
        api = ApiPerformance(apikey.client_id, apikey.client_secret, shop=apikey.shop)
        api_campaign_statistics(apikey.shop, api, days=kwargs.get("days", 1))
    except ErrorLocked:
        return {"❌FAILED": f"{apikey.shop} Locked. Skip."}
    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        logger.exception(msg)
        return {"❌FAILED": msg}

    return {"SUCCESS": f"{apikey.shop}"}


def api_campaign_statistics(shop, api, days=1):
    start_at = datetime.now()
    data = api.campaign_daily(days=days)

    logger.info(
        f"{shop} Загружена статистика рекламных кампаний: {len(data)} строк. Прошло {datetime.now() - start_at}"
    )

    msg = bulk_insert_update(
        data=data,
        key_fields=["dt", "campaign_id"],
        cls=StatisticsCampaign,
    )
    logger.info(f"{shop} {msg}")
