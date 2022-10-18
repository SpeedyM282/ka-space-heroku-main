import logging

from django.core.management.base import BaseCommand

from mp.helpers import get_keys, KEY_TYPE_OZON, MP_KEYS
from mp.models import Shop


logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Показывает список доступных активных магазинов"""

    help = "Update products from API"

    def add_arguments(self, parser):
        pass

    def handle(self, *args, **options):
        self.stdout.write(f"Активные магазины")
        shops = Shop.objects.filter(is_active=True)
        for s in shops:
            print(s)

        return
