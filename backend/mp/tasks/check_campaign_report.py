from datetime import datetime, timedelta
import json
import logging

from django.db import connection

from ka_space.celery import app
from mp.helpers import get_key

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import (
        Report,
        StatisticsCampaignOrder,
        StatisticsCampaignProduct,
    )
    from mp_ozon.api import ApiPerformance, LIMIT_DAYS, LIMIT_CAMPAIGNS
    from mp_ozon.errors import (
        ErrorRequest404,
        ErrorLocked,
        ErrorRequest,
        ErrorRateLimit,
    )
except:
    logger.warning("MP_Ozon not available")

DISABLE_APIKEY_MINUTES = 15


@app.task
def check_campaign_report(*args, apikey_id=None, **kwargs):
    """Проверка отчетов статистики рекламных кампаний

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
        check_queue(api, apikey.shop)
    except ErrorLocked:
        # отключаем ключ на несколько минут
        apikey.disabled_till = datetime.now() + timedelta(
            minutes=DISABLE_APIKEY_MINUTES
        )
        return {"❌FAILED": f"{apikey.shop} Locked. Skip."}
    except ErrorRateLimit as ex:
        # отключаем ключ на несколько минут
        apikey.disabled_till = datetime.now() + timedelta(
            minutes=DISABLE_APIKEY_MINUTES
        )
        return {"❌FAILED": f"Error: {ex}"}
    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        logger.exception(msg)
        return {"❌FAILED": msg}

    return {"SUCCESS": f"{apikey.shop}"}


def check_queue(api, shop):
    report_sent = False
    reports = Report.objects.filter(shop=shop, is_parsed=False).order_by(
        "uuid", "-updated_at"
    )[:5]
    for r in reports:
        logger.debug(f"Report: {r} ReportSent: {report_sent}")
        if r.uuid is not None and r.state == "OK":
            # Отчет готов, скачиваем
            logger.debug(f"Download Report: {r}")
            try:
                result = api.report_campaigns_download(uuid=str(r.uuid))
            except ErrorRequest404:
                # Отчет не найден
                result = {}
                r.state = "FAIL"

            for type_, lines in result.items():
                if type_ == "SKU":
                    statistics_product(lines)
                elif type_ == "SEARCH_PROMO":
                    statistics_order(lines)
                elif len(lines):
                    # показываем структуру первого элемента
                    logger.info(
                        f"Отчет: {r.uuid} от {r.created_at}\n "
                        f"Conditions: {r.conditions} Response: {r.response}"
                    )
                    logger.error(f"{shop} Неизвестный тип рекламной кампании: {type_}")

            r.is_parsed = True
            r.save()
        elif r.state == "ERROR":
            # Отчет закончен с ошибкой
            logger.debug(f"Close Report with Error")
            r.is_parsed = True
            r.save()
        elif r.uuid is not None:
            # Отчет отправлен, проверяем состояние
            logger.debug(f"Check Report: {r}")
            result = api.report_campaigns_check(uuid=str(r.uuid))

            r.response = json.dumps(result)
            r.state = result.get("state")
            if r.state != "OK":
                # Если любой другой статус, кроме OK, запрещаем отправлять новый отчет
                report_sent = True
            r.save()
        elif not report_sent:
            cond = json.loads(r.conditions)
            if not is_correct_report(cond):
                # проверяем лимиты и удаляем неудачные запросы
                logger.warning(f"Bad report conditions: {r} Remove.")
                r.delete()
                continue

            logger.debug(f"Try to send report: {r}")
            result = api.report_campaigns_request(**cond)

            if "error" in result:
                logger.error(f"{shop} Error on report request: {result}")
            elif "UUID" in result:
                report_sent = True
                r.uuid = result["UUID"]
                r.save()


def statistics_product(lines):
    """Разбираем историю открутки товаров в рекламных кампаниях и сохраняем

    :param lines:
    :return:
    """
    model_fields = [
        str(f).split(".")[-1] for f in StatisticsCampaignProduct._meta.get_fields()
    ]
    for l in lines:
        obj, created = StatisticsCampaignProduct.objects.get_or_create(
            **{
                "campaign_id": l["campaign_id"],
                "dt": l["dt"],
                "sku": l["sku"],
                "page": l.get("page", "Трафареты"),
                "condition": l.get("condition", "Трафареты"),
            }
        )
        for attr, value in {k: v for k, v in l.items() if k in model_fields}.items():
            if str(getattr(obj, attr)) != str(value):
                setattr(obj, attr, value)
        obj.save()


def statistics_order(lines):
    """Разбираем историю заказов из рекламных кампаний и сохраняем

    :param lines:
    :return:
    """
    model_fields = [
        str(f).split(".")[-1] for f in StatisticsCampaignOrder._meta.get_fields()
    ]

    campaign_ids = set()
    for l in lines:
        campaign_ids.add(l["campaign_id"])
        try:
            obj, created = StatisticsCampaignOrder.objects.get_or_create(
                **{
                    "order_id": l["order_id"],
                    "sale_product_sku": l["sale_product_sku"],
                    "campaign_id": l["campaign_id"],
                }
            )
        except StatisticsCampaignOrder.MultipleObjectsReturned as ex:
            logger.exception(
                f"Error: {ex} Remove orders of campaign {l['campaign_id']}."
            )
            continue
        for attr, value in {k: v for k, v in l.items() if k in model_fields}.items():
            if str(getattr(obj, attr)) != str(value):
                setattr(obj, attr, value)
        obj.save()

    if campaign_ids:
        update_product_statistics(campaign_ids)


def update_product_statistics(campaign_ids):
    sql = """
    -- добавляем статистику по заказам артикула из статистики заказов 
    INSERT INTO mp_ozon_statisticscampaignproduct 
    (dt, sku, price, views, clicks, expense, orders, revenue, 
     campaign_id, page, condition, created_at, updated_at)
    (
        SELECT 
            moo.dt, moo.sale_product_sku, AVG(moo.price), 
            0 as views, 0 as clicks, 
            SUM(moo.count * moo.rate_amount) as expense, SUM(moo.count) as orders, 
            SUM(moo.count * moo.price) as revenue, moo.campaign_id,
            'Продвижение', 'SEARCH_PROMO', NOW(), NOW()
        FROM mp_ozon_statisticscampaignorder moo
        WHERE moo.campaign_id in %(campaign_ids)s
        GROUP BY moo.dt, moo.sale_product_sku, moo.campaign_id
    ) ON CONFLICT (campaign_id, dt, sku, page, condition) DO UPDATE SET 
        price = excluded.price, 
        expense = excluded.expense,
        orders = excluded.orders,
        revenue = excluded.revenue,
        updated_at = NOW();
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, {"campaign_ids": tuple(campaign_ids)})

    logger.debug(f"Статистика продвижения в поиске обновлена для {campaign_ids}.")


def is_correct_report(conditions):
    """Проверяем условия рекламных кампаний

    :param conditions:
    :return:
    """
    params = conditions["params"]
    date_from = datetime.strptime(params["dateFrom"], "%Y-%m-%d")
    date_to = datetime.strptime(params["dateTo"], "%Y-%m-%d")
    is_correct = (date_to - date_from).days <= LIMIT_DAYS and len(
        params["campaigns"]
    ) <= LIMIT_CAMPAIGNS

    return is_correct
