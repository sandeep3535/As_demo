from motor.motor_asyncio import AsyncIOMotorClient
import asyncio


class Mongomotor:
    def __init__(self):
        self.client = AsyncIOMotorClient("mongodb://localhost:27017")
        self.db = self.client["AlgoSuccess"]

    async def updateData(self, collection_name: str, update_filter: dict, update_value: dict):
        update_data = await self.db[collection_name].update_one(update_filter, {"$set": update_value})
        return update_data

    async def findOneData(self, collection_name: str, find_filter: dict=None, value_filter: dict=None):
        find_data = await self.db[collection_name].find_one(find_filter, value_filter)
        return find_data

    async def findManyData(self, collection_name: str, find_filter: dict=None, value_filter: dict=None):
        find_data = self.db[collection_name].find(find_filter, value_filter)
        return [data async for data in find_data]

    async def insertData(self, collection_name: str, insert_data: list):
        data = await self.db[collection_name].insert_many(insert_data, ordered=False)
        return data


if __name__ == '__main__':
    asyncio.run(Mongomotor().updateData(collection_name="login_credential",
                                        update_filter={"broker_name": "Angel One"},
                                        update_value={'feed_token': '0871681170'}
                                        )
                )
