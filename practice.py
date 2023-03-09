# import sqlite3
# import pandas as pd
#
# conn = sqlite3.connect("D:\Projects\AS Paper\store\AlgoSuccess.db")
# df = pd.read_sql_query(sql="SELECT * FROM instruments_angel_one", con=conn)
# conn.commit()
# conn.close()
#
# pd.set_option("display.max_columns", None)
# # df.index = df["SYMBOL"]
# print()
import asyncio
import pandas as pd
import aiohttp
from io import StringIO


async def getRawInstruments() -> str:
    link = "https://preferred.kotaksecurities.com/security/production/TradeApiInstruments_Cash_23_02_2023.txt"
    async with aiohttp.ClientSession() as session:
        data = await session.get(url=link)
        response = await data.text()
    return response


async def getInstruments():
    use_cols = ["instrumentToken", "instrumentName", "name", "expiry", "strike",
                "lotSize", "instrumentType", "exchange", "tickSize", "OptionType"]
    instrument_file = StringIO(await getRawInstruments())
    df = pd.read_csv(instrument_file, sep="|", usecols=use_cols)
    df = df.rename(columns={"instrumentToken": "TOKEN", "instrumentName": "SYMBOL",
                            "name": "NAME", "expiry": "EXPIRY",
                            "strike": "STRIKE", "lotsize": "LOTSIZE",
                            "instrumentType": "INSTRUMENT_TYPE",
                            "exchange": "EXCHANGE", "tickSize": "TICK_SIZE", "OptionType": "OPTION_TYPE"})
    df["OPTION_TYPE"] = df["OPTION_TYPE"].replace(["- "], None)
    return df.head()


print(asyncio.run(getRawInstruments()))
