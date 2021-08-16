from db import get_collection
import pymongo
import asyncio
class Watcher:
    def __init__(self, collection, max_conn=16, resume_token=None):
        self.loop = asyncio.new_event_loop()
        self.col = get_collection(collection, event_loop=self.loop)
        self.max_conn = max_conn
        self.resume_token = None
    
    def run(self):
        self.loop.run_until_complete(self._run())
    
    async def _run(self):
        async with asyncio.Semaphore(self.max_conn) as sem:
            pipeline = [{'$match': {'operationType': 'insert'}}]
            try:
                async with self.col.watch(pipeline, resume_after=self.resume_token) as stream:
                    async for insert_change in stream:
                        await self.work(insert_change)
                        self.resume_token = stream.resume_token
            except pymongo.errors.PyMongoError:
                # The ChangeStream encountered an unrecoverable error or the
                # resume attempt failed to recreate the cursor.
                if self.resume_token is None:
                    # There is no usable resume token because there was a
                    # failure during ChangeStream initialization.
                    print('cannot recover...')
                else:
                    # Use the interrupted ChangeStream's resume token to
                    # create a new ChangeStream. The new stream will
                    # continue from the last seen insert change without
                    # missing any events.
                    async with self.col.watch(
                            pipeline, resume_after=self.resume_token) as stream:
                        async for insert_change in stream:
                            await self.work(insert_change)
        
    async def work(self, new_item):
        '''async work method
        call other api or do dome io bound tasks
        '''
        await asyncio.sleep(0.1)
        print(f'elapsed: 100ms...', new_item.get('fullDocument', {}).get('item'))


if __name__ == '__main__':
    watcher = Watcher('stream_check')
    watcher.run()
