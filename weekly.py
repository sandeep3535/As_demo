from mongo_data import Mongomotor
import asyncio
from datetime import datetime, timedelta
import calendar
from sqlite_data import SqliteDB
from bson.objectid import ObjectId


class StoreWeeklyUser:
    def __init__(self):
        self.mongo_db = Mongomotor()
        self.admin = None

    async def getStrategyTimeData(self):
        if not self.admin:
            self.admin = await self.mongo_db.findOneData(collection_name="users",
                                                         find_filter={"is_super_admin": True},
                                                         value_filter={"_id": 1})

        strategy_time = await self.mongo_db.findManyData(collection_name="paper_strategies_statistics",
                                                         find_filter={"user_id": str(self.admin["_id"]),
                                                                      "status": True},
                                                         value_filter={"_id": 0, "strategy_type": 1,
                                                                       "start_day": 1,
                                                                       "start_time": 1,
                                                                       "code": 1,
                                                                       "days_delta": 1,
                                                                       "duration_delta": 1
                                                                       })
        for data in strategy_time:
            if data['strategy_type'] == 'intraday':
                data['start_time'] = datetime.strptime(await self.getIntradayTime(start_time=data['start_time'],
                                                                                  day_delta=data['days_delta']),
                                                       "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'carry_forward':
                data['start_time'] = datetime.strptime(await self.getIntradayTime(start_time=data['start_time'],
                                                                                  day_delta=data['start_time']),
                                                       "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'weekly':
                data['start_time'] = datetime.strptime(
                    await self.getWeeklyTime(start_day=data['start_day'],
                                             start_time=data['start_time'],
                                             day_delta=data['days_delta'],
                                             duration_delta=data['duration_delta']),
                    "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'monthly':
                data['start_time'] = datetime.strptime(
                    await self.getMonthlyTime(start_day=data['start_day'],
                                              start_time=data['start_time'],
                                              day_delta=data['days_delta'],
                                              duration_delta=data['duration_delta']),
                    "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
        return strategy_time

    async def isHoliday(self, date_time: datetime = None) -> bool:
        if date_time is None:
            date_time = datetime.today()
        day = calendar.day_name[date_time.weekday()]
        date_time = date_time.strftime("%d-%m-%Y")
        holiday = await self.mongo_db.findOneData(collection_name="holidays", find_filter={"date": date_time})
        if day in ["Saturday", "Sunday"] or holiday:
            return True
        else:
            return False

    async def getIntradayTime(self, start_time: str, datetimeobj: datetime = None, day_delta=0):
        start_time = datetime.strptime(start_time, "%H:%M:%S").time()
        datetimeobj = datetime.now() if datetimeobj is None else datetimeobj
        datetimeobj = datetimeobj + timedelta(days=day_delta)
        if datetimeobj < datetime.now():
            datetimeobj = datetimeobj - timedelta(days=day_delta)
            datetimeobj = datetimeobj + timedelta(days=1)
        datetimeobj = datetimeobj.replace(hour=start_time.hour,
                                          minute=start_time.minute,
                                          second=start_time.second,
                                          microsecond=start_time.microsecond)
        while await self.isHoliday(datetimeobj):
            datetimeobj = datetimeobj + timedelta(days=1)
        if datetimeobj <= datetime.now():
            day_delta += 1
            return await self.getIntradayTime(start_time=str(start_time), day_delta=day_delta)
        return datetimeobj

    async def getWeeklyTime(self, start_day: str, start_time: str, duration_delta=0, day_delta=0):
        weekdays = [day for day in calendar.day_name]
        next_weekday = weekdays.index(start_day.capitalize())
        today = datetime.today().weekday()
        days_until_next_weekday = (next_weekday - today) % 7
        next_weekday_date = datetime.today() + timedelta(days=days_until_next_weekday)
        next_weekday_datetime_str = f'{next_weekday_date.strftime("%Y-%m-%d")} {start_time}'
        next_weekday_datetime = datetime.strptime(next_weekday_datetime_str, '%Y-%m-%d %H:%M:%S')
        next_weekday_datetime = next_weekday_datetime + timedelta(days=7 * duration_delta)
        next_weekday_datetime = next_weekday_datetime + timedelta(days=day_delta)
        while await self.isHoliday(next_weekday_datetime):
            next_weekday_datetime = next_weekday_datetime - timedelta(days=1)
        if next_weekday_datetime <= datetime.now():
            duration_delta += 1
            return await self.getWeeklyTime(start_day=start_day, start_time=start_time, duration_delta=duration_delta,
                                            day_delta=day_delta)
        return next_weekday_datetime

    async def getMonthlyTime(self, start_day: str, start_time: str, day_delta=0, duration_delta=0):
        now = datetime.now() + timedelta(days=30 * duration_delta)
        start_time = datetime.strptime(start_time, "%H:%M:%S").time()
        last_day = datetime(now.year, now.month, 1) + timedelta(days=32)
        last_day = last_day.replace(day=1) - timedelta(days=1)
        for i in range(last_day.day, 0, -1):
            date = datetime(now.year, now.month, i)
            if date.strftime('%A') == start_day:
                next_month_datetime = datetime.combine(date, start_time)
                next_month_datetime = next_month_datetime + timedelta(days=day_delta)
                while await self.isHoliday(next_month_datetime):
                    next_month_datetime = next_month_datetime - timedelta(days=1)
                if next_month_datetime <= datetime.now():
                    duration_delta += 1
                    return await self.getMonthlyTime(start_day=start_day, start_time=str(start_time),
                                                     day_delta=day_delta,
                                                     duration_delta=duration_delta)
                return next_month_datetime

    async def storeDataSqlite(self):
        sqlitedb = SqliteDB()
        if not sqlitedb.checkTableExists(table_name="Strategies"):
            sqlitedb.createTable(table_name="Strategies", columns=("code", "start_time"))
            store_strategies = await self.getStrategyTimeData()
            sqlitedb.insertData(table_name='Strategies', data=store_strategies)
        else:
            store_strategies = await self.getStrategyTimeData()
            sqlitedb.insertData(table_name='Strategies', data=store_strategies)

    @staticmethod
    async def checkStrategyTime(start_time: datetime) -> bool:
        time_dif = start_time.timestamp() - datetime.now().timestamp()
        if start_time.date() == datetime.today().date():
            if time_dif > 0:
                await asyncio.sleep(delay=time_dif)
            else:
                return False
            return True
        else:
            return False

    async def getStrategyUser(self):
        sqlitedb = SqliteDB()
        data = sqlitedb.findData(table_name='Strategies', column='*', condition="date(start_time)=date('now')")
        print(data)
        strategy_user = []
        for strategy in data:
            if await self.checkStrategyTime(strategy['start_time']):
                user_data = await self.mongo_db.findManyData(collection_name="paper_strategies_statistics",
                                                             find_filter={"code": strategy['code']},
                                                             value_filter={"_id": 1, "user_id": 1, "min_margin": 1,
                                                                           "trade_duration": 1, "code": 1,
                                                                           "total_active_days": 1})
                for data in user_data:
                    strategy_user.append(data)

        for data in strategy_user:
            user_data = await self.mongo_db.findManyData(collection_name="user_strategy",
                                                         find_filter={"strategy_id": str(data["_id"])},
                                                         value_filter={"_id": 0, "strategy_id": 1, "trade_type": 1,
                                                                       "broker_id": 1, "lot_size": 1})
            for _data in user_data:
                data.update(_data)
        for data in strategy_user:
            user_name = await self.mongo_db.findManyData(collection_name="users",
                                                         find_filter={"_id": ObjectId(data['user_id'])},
                                                         value_filter={"_id": 0, "first_name": 1, "last_name": 1})
            for _data in user_name:
                data["name"] = f"{_data['first_name']} {_data['last_name']}"
                del data['_id']
        return strategy_user

    async def storeStrategyUser(self) -> None:
        sqlitedb = SqliteDB()
        await self.storeDataSqlite()
        user_data = await self.getStrategyUser()

        for data in user_data:
            if sqlitedb.checkTableExists(table_name=data['code']):
                sqlitedb.insertData(table_name=data['code'], data=data)
            else:
                sqlitedb.createTable(table_name=data['code'], columns=tuple(data.keys()))
                sqlitedb.insertData(table_name=data['code'], data=data)


if __name__ == '__main__':
