import pprint
from datetime import datetime, date
import logging

from django.db.utils import IntegrityError
from django.db.models import Q

from ka_space.celery import app
from api.helpers import Update_Daily
from mp.helpers import get_key

logger = logging.getLogger(__name__)

try:
    from mp_ozon.models import (
        SKU_Offer,
        CampaignProduct,
        Campaign,
        CampaignProduct_History,
    )
    from mp_ozon.api import ApiPerformance
    from mp_ozon.errors import ErrorRequest404, ErrorLocked
except:
    logger.warning("MP_Ozon not available")


@app.task
def update_campaigns(*args, apikey_id=None, **kwargs):
    """Обновление рекламных кампаний магазина

    ! Вызывается до обновления статистики из задачи update_campaign_statistics

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
        api_campaigns(apikey.shop, api)
    except ErrorLocked:
        return {"❌FAILED": f"{apikey.shop} Locked. Skip."}
    except Exception as ex:
        msg = f"Shop: {apikey.shop} Error: {ex}"
        logger.exception(msg)
        return {"❌FAILED": msg}

    # обновление daily-статистики
    Update_Daily.campaigns(params={"shop_id": apikey.shop.pk})

    return {"SUCCESS": f"{apikey.shop}"}


def api_campaigns(shop, api):
    start_at = datetime.now()
    campaigns = api.campaigns()

    logger.info(
        f"{shop} Загружены рекламные кампании: {len(campaigns)} строк. Прошло {datetime.now() - start_at}"
    )

    campaign_model_fields = [str(f).split(".")[-1] for f in Campaign._meta.get_fields()]
    campaign_product_model_fields = [
        str(f).split(".")[-1] for f in CampaignProduct._meta.get_fields()
    ]
    for c in campaigns:
        changed_fields = []

        campaign, created = Campaign.objects.get_or_create(**{"id": c["id"]})

        if created:
            campaign.shop = shop
        elif campaign.shop != shop:
            logger.error(
                f"Кампания {campaign} принадлежит другому магазину: {campaign.shop}. "
                f"Меняем магазин на {shop}."
            )
            campaign.shop = shop
            logger.info(f"Теперь кампания {campaign} принадлежит {campaign.shop}.")
            changed_fields.append("shop")

        for attr, value in {
            k: v for k, v in c.items() if k in campaign_model_fields
        }.items():
            if attr[-3:] == "_at":
                # convert to datetime
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
            if str(getattr(campaign, attr)) != str(value):
                changed_fields.append(attr)
                setattr(campaign, attr, value)
        if changed_fields:
            campaign.save()
            logger.debug(f"{campaign} Изменено: {changed_fields}")

        if campaign.state != "CAMPAIGN_STATE_RUNNING":
            # пропускаем выключенные кампании
            continue

        # привязываем товары
        linked_products = api.campaign_products(
            c["id"], campaign_type=c["adv_type"], campaign_state=c["state"]
        )
        for p in linked_products:
            try:
                sku_offer = SKU_Offer.objects.get(sku=p["sku"])
            except SKU_Offer.DoesNotExist:
                logger.error(
                    f"{shop} Product not found. SKU={p['sku']} CampaignId={c['id']}"
                )
                continue
            (campaign_product, created,) = CampaignProduct.objects.get_or_create(
                **{"campaign": campaign, "product": sku_offer.product}
            )

            changed_fields = []
            for attr, value in {
                k: v for k, v in p.items() if k in campaign_product_model_fields
            }.items():
                if str(getattr(campaign_product, attr)) != str(value):
                    changed_fields.append(attr)
                    setattr(campaign_product, attr, value)
            if len(changed_fields):
                campaign_product.save()
                logger.debug(
                    f"{campaign} Добавлен товар: {campaign_product} Изменено: {changed_fields}"
                )

            # обновляем историю рекламных настроек товара
            if "visibility_idx" in p:
                try:
                    key = {
                        **{
                            "date": date.today(),
                            "product": sku_offer.product,
                            "campaign": campaign,
                        }
                    }
                    history, created = CampaignProduct_History.objects.get_or_create(
                        **key
                    )
                except IntegrityError as ex:
                    logger.error(
                        f"{shop} Ошибка обновления рекламных настроек: {ex} {p}"
                    )
                    continue

                history.bid = p["bid"]
                history.visibility_idx = p["visibility_idx"]
                history.save()
                logger.debug(f"{campaign} Обновлена история товара: {campaign_product}")

        CampaignProduct.objects.filter(campaign=campaign).exclude(
            sku__in=[p["sku"] for p in linked_products]
        ).delete()
        logger.debug(f"{campaign} Удалены старые связи товаров")

    logger.info(
        f"{shop} Рекламные кампании. Обновлено {len(campaigns)} строк. Прошло {datetime.now() - start_at}"
    )
