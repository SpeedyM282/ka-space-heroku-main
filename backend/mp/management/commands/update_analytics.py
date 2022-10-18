import logging

from django.core.management.base import BaseCommand

from mp.helpers import get_keys, KEY_TYPE_OZON, MP_KEYS

from mp.tasks import update_analytics

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Update analytics from API"

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=2)
        parser.add_argument("--days_step", type=int, default=None)
        parser.add_argument("--shop_id", type=int, default=0)

    def handle(self, *args, **options):
        for mp in MP_KEYS:
            apikeys = get_keys(
                mp, key_type=KEY_TYPE_OZON, shop_id=options.get("shop_id", 0)
            )

            for apikey in apikeys:
                self.stdout.write(
                    f"Отправляем задачу обновления аналитики магазина {apikey.type} / {apikey.shop}..."
                )
                update_analytics.delay(
                    apikey_id=apikey.pk,
                    days=options.get("days"),
                    days_step=options.get("days_step"),
                    shop_id=apikey.shop_id,
                )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Задачи обновления для маркетплейса {mp.upper()} успешно поставлены."
                )
            )
