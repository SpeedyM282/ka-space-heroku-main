import pprint
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import logging
import random

from django.db import transaction
from django.utils import timezone

from mp.models import APIKey

logger = logging.getLogger(__name__)

MP_KEYS = [
    "ozon",
]  # "wb"

KEY_TYPE_OZON = "ozon"
KEY_TYPE_OZON_PERFORMANCE = "performance"

SLOW_TASK_TIMEOUT = 60 * 5


def get_keys(mp, key_type, shop_id=None):
    """Returns API Keys objects

    :param mp:
    :param key_type:
    :param shop_id:
    :return:
    """
    _filter = {
        "type": key_type,
        "is_active": True,
        "disabled_till__lt": timezone.now(),
        "shop__shop_token": mp,
        "shop__is_active": True,
    }
    try:
        if shop_id:
            _filter["shop__id"] = shop_id
        keys = APIKey.objects.filter(**_filter).order_by("shop__id")

        # group by shops
        shops = defaultdict(list)
        for key in keys:
            shops[key.shop].append(key)

        # select random key, one for shop
        keys = []
        for shop, apikeys in shops.items():
            keys.append(apikeys[0] if len(apikeys) == 1 else random.choice(apikeys))
    except APIKey.DoesNotExist:
        keys = []

    if not keys:
        logger.info(f"Для {mp} отсутствуют API-ключи.")

    return keys


def get_key(pk=None):
    """Returns API Key by Id

    :param pk:
    :return:
    """
    _filter = {
        "pk": pk,
        "is_active": True,
        "disabled_till__lt": timezone.now(),
        "shop__is_active": True,
    }

    key = APIKey.objects.get(**_filter)

    return key


def chunks(l, n):
    """
    Split lists to chunks by N items

    :param l:
    :param n:
    :return:
    """
    n = max(1, n)
    return (l[i : i + n] for i in range(0, len(l), n))


def bulk_insert_update(
    data=[], key_fields=[], changed_or_skip_func=None, cls=None, shop=None
):
    keys = {k: set(str(r[k]) for r in data) for k in key_fields}
    filter_existing = {
        **{f"{k}__in": v for k, v in keys.items()},
        **({"shop": shop} if shop else {}),
    }
    id_rows = {tuple([str(r[k]) for k in key_fields]): r for r in data}

    cls_model_fields = [str(f).split(".")[-1] for f in cls._meta.get_fields()]

    with transaction.atomic():
        existing_objs = {
            tuple([str(getattr(obj, k)) for k in key_fields]): obj
            for obj in cls.objects.filter(**filter_existing).select_for_update()
        }

        # new items
        create_data = {
            key: {k: v for k, v in obj.items() if k in cls_model_fields}
            for key, obj in id_rows.items()
            if key not in existing_objs
        }
        creates = [
            cls(**{**obj_data, **({"shop": shop} if shop else {})})
            for obj_data in create_data.values()
        ]
        if creates:
            cls.objects.bulk_create(creates)

        # update items
        update_fields = set()
        updates = []
        for key, obj in existing_objs.items():
            skip_update = False
            changed_fields = []
            for attr, value in {
                k: v for k, v in id_rows[key].items() if k in cls_model_fields
            }.items():
                changed = getattr(obj, attr) != value
                if type(getattr(obj, attr)) in [date, int]:
                    # check for date & int
                    changed = str(getattr(obj, attr)) != str(value)
                elif type(getattr(obj, attr)) == datetime:
                    # check for datetime
                    changed = getattr(obj, attr) != datetime.fromisoformat(value)
                elif type(getattr(obj, attr)) == Decimal:
                    # check for decimal
                    changed = f"{getattr(obj, attr):.5f}" != f"{float(value):.5f}"

                if callable(changed_or_skip_func):
                    changed, skip = changed_or_skip_func(changed, obj, attr, value)
                    if skip:
                        print(f"Skip update {obj}")
                        skip_update = True

                if changed:
                    # print(
                    #     f"~~~{attr}: {type(getattr(obj, attr))} {getattr(obj, attr)} => {type(value)} {value}"
                    # )
                    changed_fields.append(attr)
                    setattr(obj, attr, value)

            if changed_fields and not skip_update:
                # pprint.pprint(id_rows[key])
                update_fields.update(changed_fields)
                updates.append(obj)
        update_result = 0
        if existing_objs and update_fields:
            update_result = cls.objects.bulk_update(updates, update_fields)

    return (
        f"Получено {len(id_rows)} "
        f"/ Найдено {len(existing_objs)} и обновлено {update_result} "
        f"/ Изменения в полях {update_fields} "
        f"/ Добавлено {len(create_data)} "
    )
