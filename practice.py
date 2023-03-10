import asyncio
import calendar
from datetime import datetime, timedelta

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

async def getMonthlyTime(self, start_day=None, start_time=None):
    current_date = datetime.today().date()
    day_list = [day for day in calendar.day_name]
    _date = calendar.monthrange(current_date.year, current_date.month)
    day = _date[1]
    last_date = datetime(current_date.year, current_date.month, day)
    last_day_num = last_date.weekday()
    if last_day_num == day_list.index(start_day):
        plus_days = 0
    elif last_day_num > day_list.index(start_day):
        plus_days = day_list.index(start_day) - last_day_num
    else:
        plus_days = day_list.index(start_day) - last_day_num - 7

    last_date = last_date + timedelta(days=plus_days)
    while await self.isHoliday(last_date):
        last_date = last_date - timedelta(days=1)
    start_time = datetime.strptime(start_time, "%H:%M:%S")
    last_date = last_date.replace(hour=start_time.hour,
                                  minute=start_time.minute,
                                  second=start_time.second,
                                  microsecond=start_time.microsecond).strftime("%d-%m-%Y %H:%M:%S")
    return last_date

async def getIntradayTime(self, start_time=None):
    execute_date = datetime.now()

    execute_date = execute_date.replace(hour=int(start_time[:2]),
                                        minute=int(start_time[3:5]),
                                        second=int(start_time[-2]),
                                        microsecond=0).strftime("%d-%m-%Y %H:%M:%S")
    execute_date_timestamp = datetime.strptime(execute_date, "%d-%m-%Y %H:%M:%S").timestamp()
    if datetime.now().timestamp() > execute_date_timestamp:
        new_date = datetime.fromtimestamp(execute_date_timestamp) + timedelta(days=1)
        while await self.isHoliday(new_date):
            new_date = new_date + timedelta(days=1)
        new_date = new_date.replace(hour=int(start_time[:2]),
                                    minute=int(start_time[3:5]),
                                    second=int(start_time[-2]),
                                    microsecond=0).strftime("%d-%m-%Y %H:%M:%S")
        return new_date
    else:
        execute_date = datetime.strptime(execute_date, "%d-%m-%Y %H:%M:%S")
        while await self.isHoliday(execute_date):
            execute_date = execute_date + timedelta(days=1)
        start_time = datetime.strptime(start_time, "%H:%M:%S")
        execute_date = execute_date.replace(hour=start_time.hour,
                                            minute=start_time.minute,
                                            second=start_time.second,
                                            microsecond=start_time.microsecond).strftime("%d-%m-%Y %H:%M:%S")
        return execute_date

async def getWeeklyTime(self, start_day=None, start_time=None, day_duration=0):
    current_date = datetime.now()
    day_list = [day for day in calendar.day_name]
    current_weekday = current_date.weekday()
    if current_weekday == day_list.index(start_day):
        plus_days = 7
    elif current_weekday > day_list.index(start_day):
        plus_days = (7 - (current_weekday - day_list.index(start_day)))
    else:
        plus_days = day_list.index(start_day) - current_weekday

    next_week_date = current_date + timedelta(days=plus_days) + timedelta(days= 7*day_duration)

    while await self.isHoliday(next_week_date):
        next_week_date = next_week_date - timedelta(days=1)
    start_time = datetime.strptime(start_time, "%H:%M:%S")
    next_week_date = next_week_date.replace(hour=start_time.hour,
                                            minute=start_time.minute,
                                            second=start_time.second,
                                            microsecond=start_time.microsecond).strftime("%d-%m-%Y %H:%M:%S")

    return next_week_date


print(asyncio.run(getRawInstruments()))
