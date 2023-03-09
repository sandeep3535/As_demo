import asyncio
import sqlite3
from pprint import pprint

import aiohttp
import pandas as pd
from angel_connect_api import AngelConnect
from mongo_data import Mongomotor
from utils import Utils


class AngelLogin:
    def __init__(self):
        self.angel = AngelConnect('BpCELprq')
        self.mongo_db = Mongomotor()

    async def userLogin(self):
        login_data = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "client_id": 1, "password": 1,
                                                                "secret_key": 1}
                                                  )

        res = await self.angel.generateLoginsession(clientcode=login_data["client_id"],
                                                    passwd=await Utils.decodeBase64(data=login_data["password"]),
                                                    secret_key=login_data["secret_key"])

        await self.mongo_db.updateData(collection_name="login_credential",
                                       update_filter={"broker_name": "Angel One"},
                                       update_value={"token": res['data']['jwtToken'],
                                                     "refresh_token": res['data']['refreshToken'],
                                                     "feed_token": res['data']['feedToken'],
                                                     "status": res['status'],
                                                     "updated_at": await Utils.getCurrentDateTime()
                                                     }
                                       )

        return res

    async def userProfile(self):
        auth_token = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "token": 1}
                                                  )

        res = await self.angel.getProfile(auth_token=auth_token["token"])
        return res

    @staticmethod
    async def getInstruments():
        link = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
        async with aiohttp.ClientSession() as session:
            async with session.request(method="GET",
                                       url=link) as response:
                res = await response.json()
        df = pd.DataFrame.from_dict(res.copy())
        df["OPTION_TYPE"] = "None"
        df = df.rename(columns={"token": "TOKEN", "symbol": "SYMBOL",
                                "name": "NAME", "expiry": "EXPIRY",
                                "strike": "STRIKE", "lotsize": "LOTSIZE",
                                "instrumenttype": "INSTRUMENT_TYPE",
                                "exch_seg": "EXCHANGE", "tick_size": "TICK_SIZE"})
        conn = sqlite3.connect("D:\Projects\AS Paper\store\AlgoSuccess.db")
        query = "Create table if not Exists instruments_angel_one (TOKEN, SYMBOL, NAME, EXPIRY, STROKE, LOTSIZE," \
                "INSTRUMENT_TYPE, EXCHANGE, TICK_SIZE, OPTION_TYPE)"
        conn.execute(query)
        df.to_sql("instruments_angel_one", conn, if_exists='replace', index=False)
        conn.commit()
        conn.close()
        return type(res)

    async def orderBook(self):
        auth_token = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "token": 1}
                                                  )
        res = await self.angel.getOrderBook(auth_token=auth_token["token"])
        result = res.copy()
        await self.mongo_db.insertData(collection_name="Order_Book", insert_data=result["data"])
        return res


    async def getLTP(self):
        auth_token = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "token": 1}
                                                  )
        res = await self.angel.getLTPData(exchange="NSE", tradingsymbol="GOLDIAM-EQ", symboltoken="11971",
                                          auth_token=auth_token["token"])
        return res

    async def placeAngelOrder(self):
        auth_token = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "token": 1})
        data = {"variety": "NORMAL", "tradingsymbol": "BANKNIFTY23MAR2342500CE", "transactiontype": "BUY",
                "symboltoken": "41666",
                "exchange": "NFO", "ordertype": "MARKET", "producttype": "CARRYFORWARD", "duration": "DAY",
                "price": "217.55", "squareoff": "0.00", "stoploss": "0.00", "quantity": "25", "ordertag": "NAF55"}

        res = await self.angel.placeOrder(params=data, auth_token=auth_token["token"])
        return res

    async def getRMSData(self):
        auth_token = await self.mongo_db.findData(collection_name="login_credential",
                                                  find_filter={"broker_name": "Angel One"},
                                                  value_filter={"_id": 0, "token": 1})
        res = await self.angel.getRMS(auth_token=auth_token["token"])
        return res


if __name__ == '__main__':
    pprint(asyncio.run(AngelLogin().getInstruments()))
