import asyncio
import logging
import sys

from aioprocessworker import AsyncWorker, get_parent_process

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(process)d] %(name)s: %(message)s")

logger = logging.getLogger()


async def client_co():
    for _ in range(3):
        logger.info("Child co")
        await asyncio.sleep(1)

    parent = get_parent_process()
    if parent:
        f = parent.schedule_coroutine(parent_co, )
        res = await f
        assert res == "kek"

    return {"swag": 123}


async def parent_co():
    for _ in range(3):
        logger.info("Server co")
        await asyncio.sleep(1)
    return "kek"


async def t1():
    worker = AsyncWorker()
    await worker.start()

    try:
        for _ in range(1):
            f = worker.schedule_coroutine(client_co, )
            await asyncio.sleep(1)
            # f.cancel()
            if not f.cancelled():
                res: dict = await f
                print(f"First worker: {res}")
                assert res['swag'] == 123

        await asyncio.sleep(3)

    finally:
        await worker.stop()


async def t2():
    async with AsyncWorker() as worker2:
        res = await worker2.schedule_coroutine(client_co)
        print(f"Second worker: {res}")


async def main():
    await asyncio.gather(t1(), t2())


if sys.platform == "win32":
    asyncio.set_event_loop_policy(
        asyncio.WindowsProactorEventLoopPolicy())

if __name__ == "__main__":
    # mp.freeze_support()
    asyncio.run(main())
