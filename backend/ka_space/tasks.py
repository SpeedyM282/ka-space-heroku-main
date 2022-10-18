from time import sleep
from ka_space.celery import app


@app.task
def task_hello_world():
    print("start task HW")
    sleep(5)  # поставим тут задержку в 10 сек для демонстрации ассинхрности
    print("Hello World")
