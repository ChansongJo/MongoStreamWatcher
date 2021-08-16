import asyncio
import numpy as np
from db import get_collection

async def main(document_count, col):
    async with asyncio.Semaphore(16) as sem:
        for i in range(document_count):
            # gap = np.random.uniform(0, 0.5)
            await asyncio.sleep(0.01)
            col.insert_one({'item': i})


if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    col = get_collection('stream_check', event_loop=loop)
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main(400, col))