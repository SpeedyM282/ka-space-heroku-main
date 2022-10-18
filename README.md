# KA.Space

## Вступление

Цель: Получать информацию из кабинетов ОЗОНа для аналитики и помощи селлерам.

Сделать продажи наглядными, понятными и предоставить доступ к данным в формате, который
легко выгрузить в Гугл.Таблицы для визаулизации и анализа. Предоставить селлеру оперативные оповещения 
о важных событиях и нестандартных ситуациях. Дать возможность любому понимать, что с его деньгами. 

## Документация

* OZON API: https://docs.ozon.ru/api/seller/
* WB API: ...

Сборка:
* https://dev.to/koladev/django-rest-authentication-cmh
* https://github.com/koladev32/django-auth-react-tutorial
* https://github.com/koladev32/django-react-auth-app

## Структура проекта

Django:

- [ ] KA_Space - Пользователи и настройки проекта

## Запуск проекта для разработки

```
cd ~/path/to/clone
git clone https://gitlab.com/i.am.raa/ka.space.git .
pipenv install
pipenv shell
```

Настройка БД:
```
sudo -u postgres psql postgres
```

SQL-команды:
```SQL
create user developer with password '1';
alter role developer set client_encoding to 'utf8';
alter role developer set timezone to 'UTC';

create database ka_db_dev owner developer;

\q
```

Миграция БД:
```commandline
cd backend
python manage.py migrate
```

Создание суперпользователя:
```commandline
python manage.py createsuperuser
```

## Разработка

Создание приложения:
```commandline
django-admin startapp new_app
```

Обновление миграций:
```commandline
python manage.py makemigrations
python manage.py migrate
```

Откат миграций приложения:
```commandline
python manage.py migrate mp zero
```
Затем можно удалить всю историю миграций и создать новыю миграцию инициализации приложения.

### Test and Deploy

Запуск проекта:
```commandline
pipenv shell
cd backend
python manage.py runserver
```

Запуск Celery:
```commandline
celery worker --loglevel=debug --concurrency=2
```


Запуск Flower:
```commandline
celery -A ka_space flower --port=5566
celery -A ka_space flower --unix-socket=/tmp/ka-flower.sock
```

### I18N

Создание словаря:
```commandline
python manage.py makemessages -l ru
```

Компиляция словаря:
```commandline
python manage.py compilemessages -l ru
```

### ASGI

Запуска веб-сервера:
```commandline
uvicorn ka_space.asgi:application --workers 2 --port 8010
```

### CRON

- [ ] /path/to/cron.py update_products ozon

### PostgreSQL

Настройка PostrgreSQL:
```commandline
sudo -u postgres psql postgres
```

Создание пользователя проекта: 
```postgresql
create user django_user with password 'password';
alter role django_user set client_encoding to 'utf8';
alter role django_user set default_transaction_isolation to 'read committed';
alter role django_user set timezone to 'UTC';
```

Создаем БД:
```postgresql
create database django_db owner django_user;
```

Включаем необходимые расширения:
```postgresql
CREATE EXTENSION btree_gin;
```

Изменение settings.py:
```python
{
    'ENGINE': 'django.db.backends.postgresql_psycopg2',
    'NAME': 'django_db',
    'USER': 'django_user',
    'PASSWORD': 'password',
    'HOST': '127.0.0.1',
    'PORT': '5432',
}
```

Обновляем миграции.

## Команды

- [ ] python manage.py show_shops
- [ ] python manage.py update_stocks (ozon) [--shop_id] N
- [ ] python manage.py update_analytics (ozon) [--days] N [--shop_id] N
- [ ] python manage.py update_transactions (ozon) [--days] N [--shop_id] N

### Update Products

Аргумент **mp_type**:
* ozon - обновление магазинов Озон
* wb - обновление магазинов Wildberries

Команда обновления товаров во всех магазинах для определенной площадки. 
Площадки могут обрабатываться параллельно.

## Отладка запросов

```python
from mp_ozon.models import Analytics

Analytics.objects.extra(
                select={"offer_id": "mp_ozon_product.offer_id", "shop": "mp_shop.name"},
                tables=["mp_ozon_product", "mp_shop"],
                where=["mp_ozon_analytics.sku=mp_ozon_product.fbo_sku", 
                       "mp_ozon_analytics.shop_id=mp_shop.id"],
            ).filter(shop=1) .order_by("sku", "date") .values(*["offer_id", "date"])
```

***


## Authors and acknowledgment
Rumyantsev Alexander <ra@quantrum.me>

## License
Однажды, здесь появится информация о лицензии.

## Project status
Active development.
