import base64
from datetime import datetime
import asyncio


class Utils:

    @staticmethod
    async def getCurrentDateTime():
        current_time = datetime.now()
        iso_formate_entry_time = current_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        changed_format = datetime.strptime(iso_formate_entry_time, "%Y-%m-%dT%H:%M:%S.000Z")
        return changed_format

    @staticmethod
    async def decodeBase64(data: str):
        data_bytes = base64.b64decode(data)
        return data_bytes.decode("utf-8")
