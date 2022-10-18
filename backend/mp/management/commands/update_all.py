import logging
from pprint import pformat

from django.core import management
from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Update products from API"

    def add_arguments(self, parser):
        parser.add_argument("--shop_id", type=int, default=0)

    def handle(self, *args, **options):
        management.call_command("update_stocks", shop_id=options.get("shop_id", 0))
        management.call_command(
            "update_analytics", days=180, shop_id=options.get("shop_id", 0)
        )
        management.call_command(
            "update_transactions", days=180, shop_id=options.get("shop_id", 0)
        )

        management.call_command(
            "update_campaign_statistics", days=180, shop_id=options.get("shop_id", 0)
        )

        management.call_command(
            "update_orders", days=180, shop_id=options.get("shop_id", 0)
        )

        self.stdout.write(self.style.SUCCESS(f"Все данные обновлены."))
