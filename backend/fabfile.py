import time
from functools import wraps

from fabric import Connection, task


def echo_step(func):
    @wraps(func)
    def wrapper(ctx, *args, **kwargs):
        host = ctx.host if hasattr(ctx, "host") else "na"
        msg = func.__doc__.split("\n")[0]
        print("\n{:->80}".format(f" {host}:{msg}"))
        start = time.monotonic()
        result = func(ctx, *args, **kwargs)
        print(f">>> Elapsed: {time.monotonic() - start:.2f} sec.")

        return result

    return wrapper


@task
@echo_step
def production(ctx):
    """Defines production environment"""
    ctx.host = "nwpro.ru"
    ctx.shell = "bash -c"  # интерпретатор для выполнения команд на удаленном хосте
    ctx.connect_kwargs.key_filename = "~/.ssh/id_rsa"
    ctx.use_ssh_config = True  # импортируем конфигурацию нашего ssh-клиента
    ctx.project_user = (
        "quantrum"  # пользователь, от которого работает проект на удаленном сервере
    )
    ctx.base_dir = "~/"  # базовая директория для проекта
    ctx.project_path = f"{ctx.base_dir}/web/mp.quantrum.me/app"  # здесь мы сформировали абсолютное имя каталога
    ctx.django_path = f"{ctx.project_path}/backend"


@echo_step
def restart(ctx, c):
    """Restarts your application services"""
    c.run(f"touch {ctx.project_path}/restart.flag")


@echo_step
def checkout(ctx, c):
    """Pulls code on the remote servers"""
    with c.cd(ctx.project_path):
        c.run("git pull")


@echo_step
def install_packages(ctx, c):
    """Install/update packages"""

    with c.cd(ctx.project_path):
        c.run(f"pipenv install", hide="out")


@echo_step
def migrate(ctx, c):
    """Migrate DB"""

    with c.cd(ctx.django_path):
        c.run(f"./cron.py migrate")


@echo_step
def collect_static(ctx, c):
    """Update static"""

    with c.cd(ctx.django_path):
        c.run(f"./cron.py collectstatic --noinput")


@task
@echo_step
def deploy(ctx):
    """Deploys your project. This calls  'checkout', 'install_packages', 'migrate', 'collect_static', 'restart'"""
    with Connection(ctx.host, user=ctx.project_user, port=ctx.port) as c:
        checkout(ctx, c)
        install_packages(ctx, c)
        migrate(ctx, c)
        collect_static(ctx, c)
        restart(ctx, c)
