import asyncio
import time
import httpx
from django.http import HttpResponse


async def http_call_async():
    print("start")
    for num in range(1, 6):
        print(num)
        await asyncio.sleep(1)
    async with httpx.AsyncClient() as client:
        r = await client.get("https://httpbin.org")
        print(r)
    print("finish")


async def async_view(request):
    loop = asyncio.get_event_loop()
    print(loop)
    task = loop.create_task(http_call_async())
    print(task)
    return HttpResponse("Non-blocking HTTP request. Works only through ASGI.")
