from motor.motor_asyncio import AsyncIOMotorClient

MONGO_HOST = '10.113.160.78:10017'
DB = 'test'
COLLECTION = 'stream_check'
def get_collection(host=MONGO_HOST, db_name=DB, collection_name=COLLECTION, event_loop=None):
    client = AsyncIOMotorClient(MONGO_HOST, io_loop=event_loop)
    db = client[DB]
    collection = db[collection_name]
    return collection
