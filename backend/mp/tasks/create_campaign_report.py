from datetime import datetime, timedelta, date
import json
import logging
import math

from django.db.models import Q

from ka_space.celery import app
from mp.helpers import get_key, chunks

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import (
        Campaign,
        Report,
    )
    from mp_ozon.api import ApiPerformance, LIMIT_CAMPAIGNS, LIMIT_DAYS
except:
    logger.warning("MP_Ozon not available")

REPORT_OLD_DAYS = 3
MAX_STATISTICS_PERIOD = 60


@app.task
def create_campaign_report(*args, apikey_id=None, **kwargs):
    """Создание запросов отчетов для обновления статистики рекламных кампаний

    FIXME Проверять отчеты отложенными задачами без CRON

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
        api = ApiPerformance(apikey.client_id, apikey.client_secret, shop=apikey.shop)
        new_report(apikey.shop, api, days=kwargs.get("days", 1))
        remove_old_report(apikey.shop)

    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        logger.exception(msg)
        return {"❌FAILED": msg}

    return {"SUCCESS": f"{apikey.shop}"}


def new_report(shop, api, days=1):
    """Создаем новые запросы отчетов

    FIXME Запрашивать отчеты только для актуальных кампаний, которые работают или попадают в диапазон

    :param shop:
    :param api:
    :param days:
    :return:
    """
    # обрезаем историю получения статистики
    days = days if days < MAX_STATISTICS_PERIOD else MAX_STATISTICS_PERIOD

    if days < 5:
        # обновляем только активные рекламные кампании
        q = Campaign.objects.filter(
            Q(shop=shop)
            & (
                Q(state="CAMPAIGN_STATE_RUNNING")
                | Q(updated_at__gt=date.today() - timedelta(days=days))
            )
        ).values_list("id", flat=True)
    else:
        q = Campaign.objects.filter(shop=shop).values_list("id", flat=True)

    campaign_ids = sorted(list(q))
    if campaign_ids:
        date_to = date.today()
        days_max = days if days < LIMIT_DAYS else LIMIT_DAYS
        for period_offset in range(math.ceil(days / days_max)):
            date_from = date_to - timedelta(days=days_max)

            for ids in chunks(campaign_ids, LIMIT_CAMPAIGNS):
                report = api.report_campaigns_create(
                    dt=date_to, days=days_max, campaign_ids=ids
                )

                obj, created = Report.objects.get_or_create(
                    **{
                        "conditions": json.dumps(report, sort_keys=True),
                        "is_parsed": False,
                        "shop": shop,
                    }
                )
                if created:
                    logger.debug(f"Отчет поставлен в очередь: {report}")

            # смещаем конечную дату диапазона, смещение на 1 день игнорируем
            date_to = date_from
    else:
        logger.debug(f"{shop}: Рекламные кампании не найдены")


def remove_old_report(shop):
    remove_date = datetime.now() - timedelta(days=REPORT_OLD_DAYS)
    result = Report.objects.filter(shop=shop, created_at__lte=remove_date).delete()
    logger.debug(f"{shop}: Удалено {result} устаревших отчетов")
