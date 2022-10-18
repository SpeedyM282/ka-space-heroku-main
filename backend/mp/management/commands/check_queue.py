import logging
import time

from django.core import management
from django.core.management.base import BaseCommand, CommandError

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Check report queue in cycle with pauses"

    def add_arguments(self, parser):
        parser.add_argument("--count", type=int, default=10)
        parser.add_argument("--pause", type=int, default=15)
        parser.add_argument("--shop_id", type=int, default=0)

    def handle(self, *args, **options):
        pause = options.get("pause", 10)
        count = options.get("count", 1)
        for i in range(count):
            self.stdout.write(f"Цикл #{i+1} / {count}...")

            management.call_command(
                "request_campaign_report",
                action="queue",
                shop_id=options.get("shop_id", 0),
            )
            if i + 1 != count:
                self.stdout.write(f"Пауза {pause} секунд...")
                time.sleep(pause)

        self.stdout.write(self.style.SUCCESS(f"Данные обновлены."))
