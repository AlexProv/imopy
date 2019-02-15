import asyncio
from CentrisScraper import CentrisScrapper, cities
from FirestoreWorker import FirestoreWorker

async def main():
    centris_worker = CentrisScrapper(cities)
    fs_worker = FirestoreWorker()

    task_centris_worker = asyncio.create_task(centris_worker.crawl())
    task_fs_worker = asyncio.create_task(fs_worker.work())

    await task_centris_worker
    await task_fs_worker

asyncio.run(main())