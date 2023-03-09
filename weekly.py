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
                                                                       "code": 1
                                                                       })
        for data in strategy_time:
            if data['strategy_type'] == 'intraday':
                data['start_time'] = datetime.strptime(await self.getIntradayTime(start_time=data['start_time']),
                                                       "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'carry_forward':
                data['start_time'] = datetime.strptime(await self.getIntradayTime(start_time=data['start_time']),
                                                       "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'weekly':
                data['start_time'] = datetime.strptime(
                    await self.getWeeklyTime(start_day=data['start_day'], start_time=data['start_time']),
                    "%d-%m-%Y %H:%M:%S")
                del data['strategy_type']
                del data['start_day']
            elif data['strategy_type'] == 'monthly':
                data['start_time'] = datetime.strptime(
                    await self.getMonthlyTime(start_day=data['start_day'], start_time=data['start_time']),
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

    async def getWeeklyTime(self, start_day=None, start_time=None):
        current_date = datetime.now()
        day_list = [day for day in calendar.day_name]
        current_weekday = current_date.weekday()
        if current_weekday == day_list.index(start_day):
            plus_days = 7
        elif current_weekday > day_list.index(start_day):
            plus_days = (7 - (current_weekday - day_list.index(start_day)))
        else:
            plus_days = day_list.index(start_day) - current_weekday

        next_week_date = current_date + timedelta(days=plus_days)

        while await self.isHoliday(next_week_date):
            next_week_date = next_week_date - timedelta(days=1)
        start_time = datetime.strptime(start_time, "%H:%M:%S")
        next_week_date = next_week_date.replace(hour=start_time.hour,
                                                minute=start_time.minute,
                                                second=start_time.second,
                                                microsecond=start_time.microsecond).strftime("%d-%m-%Y %H:%M:%S")

        return next_week_date

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
    print(asyncio.run(StoreWeeklyUser().getWeeklyTime(start_day="Thursday", start_time="15:10:00")))
