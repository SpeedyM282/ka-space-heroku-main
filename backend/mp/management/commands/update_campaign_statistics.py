import logging

from django.core.management.base import BaseCommand

from mp.helpers import get_keys, KEY_TYPE_OZON_PERFORMANCE, MP_KEYS
from mp.tasks import update_campaign_statistics

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Обновляет статститку реклманых кампаний без подробностей

    ```
    python manage.py update_campaign_statistics --days 120
    ```

    """

    help = "Update campaigns from API"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=1)
        parser.add_argument("--shop_id", type=int, default=0)

    def handle(self, *args, **options):
        for mp in MP_KEYS:
            apikeys = get_keys(
                mp,
                key_type=KEY_TYPE_OZON_PERFORMANCE,
                shop_id=options.get("shop_id", 0),
            )

            for apikey in apikeys:
                self.stdout.write(
                    f"Отправляем задачу обновления статистики рекламных кампаний магазина "
                    f"{apikey.type} / {apikey.shop}..."
                )
                update_campaign_statistics(
                    apikey_id=apikey.pk,
                    days=options.get("days"),
                    shop_id=apikey.shop_id,
                )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Задачи обновления для маркетплейса {mp.upper()} успешно поставлены."
                )
            )
