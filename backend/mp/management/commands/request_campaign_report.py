import logging

from django.core.management.base import BaseCommand

from mp.helpers import get_keys, KEY_TYPE_OZON_PERFORMANCE, MP_KEYS
from mp.tasks import create_campaign_report, check_campaign_report
from mp_ozon.models import Report

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Ставит в очередь и обрабатывает отчеты по рекламным кампаниям

    Отчеты по рекламе за последние 3 дня:
    ```
    python manage.py request_campaign_report ozon --days 3
    ```

    Проверка и обработка очереди отчетов (каждые 1-3 минуты):
    ```
    python manage.py request_campaign_report ozon --action queue
    ```

    """

    help = "Works with campaigns' reports from API"

    def add_arguments(self, parser):
        # stat, queue
        parser.add_argument("--action", type=str, default="stat")
        parser.add_argument("--days", type=int, default=1)
        parser.add_argument("--shop_id", type=int, default=0)

    def handle(self, *args, **options):
        for mp in MP_KEYS:
            apikeys = get_keys(
                mp,
                key_type=KEY_TYPE_OZON_PERFORMANCE,
                shop_id=options.get("shop_id", 0),
            )
            action = options.get("action", "create_stat")

            for apikey in apikeys:
                if action == "stat":
                    self.stdout.write(
                        f"Отправляем задачу создания запросов рекламных отчетов "
                        f"магазина {apikey.type} / {apikey.shop}..."
                    )
                    create_campaign_report.delay(
                        apikey_id=apikey.pk,
                        days=options.get("days"),
                        shop_id=apikey.shop_id,
                    )
                elif action == "queue":
                    active_reports = Report.objects.filter(
                        shop=apikey.shop, is_parsed=False
                    )
                    if len(active_reports):
                        self.stdout.write(
                            f"Отправляем задачу проверки рекламных отчетов "
                            f"магазина {apikey.type} / {apikey.shop}..."
                        )
                        check_campaign_report.delay(
                            apikey_id=apikey.pk, shop_id=apikey.shop_id
                        )

            self.stdout.write(
                self.style.SUCCESS(
                    f"Задачи обработки рекламных отчетов {mp.upper()} успешно поставлены."
                )
            )
