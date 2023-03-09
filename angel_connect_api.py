import asyncio
import datetime
import json
import re
import socket
import time
import uuid
from urllib.parse import urljoin

import aiohttp
import pyotp
from requests import get


class AngelConnect:
    _rootUrl = "https://apiconnect.angelbroking.com"
    _login_url = "https://smartapi.angelbroking.com/publisher-login"

    _routes = {
        "api.login": "/rest/auth/angelbroking/user/v1/loginByPassword",
        "api.logout": "/rest/secure/angelbroking/user/v1/logout",
        "api.token": "/rest/auth/angelbroking/jwt/v1/generateTokens",
        "api.refresh": "/rest/auth/angelbroking/jwt/v1/generateTokens",
        "api.user.profile": "/rest/secure/angelbroking/user/v1/getProfile",

        "api.order.place": "/rest/secure/angelbroking/order/v1/placeOrder",
        "api.order.modify": "/rest/secure/angelbroking/order/v1/modifyOrder",
        "api.order.cancel": "/rest/secure/angelbroking/order/v1/cancelOrder",
        "api.order.book": "/rest/secure/angelbroking/order/v1/getOrderBook",

        "api.ltp.data": "/rest/secure/angelbroking/order/v1/getLtpData",
        "api.trade.book": "/rest/secure/angelbroking/order/v1/getTradeBook",
        "api.rms.limit": "/rest/secure/angelbroking/user/v1/getRMS",
        "api.holding": "/rest/secure/angelbroking/portfolio/v1/getHolding",
        "api.position": "/rest/secure/angelbroking/order/v1/getPosition",
        "api.convert.position": "/rest/secure/angelbroking/order/v1/convertPosition",

        "api.gtt.create": "/gtt-service/rest/secure/angelbroking/gtt/v1/createRule",
        "api.gtt.modify": "/gtt-service/rest/secure/angelbroking/gtt/v1/modifyRule",
        "api.gtt.cancel": "/gtt-service/rest/secure/angelbroking/gtt/v1/cancelRule",
        "api.gtt.details": "/rest/secure/angelbroking/gtt/v1/ruleDetails",
        "api.gtt.list": "/rest/secure/angelbroking/gtt/v1/ruleList",

        "api.candle.data": "/rest/secure/angelbroking/historical/v1/getCandleData"
    }
    try:
        clientPublicIp = " " + get('https://api.ipify.org').text
        if " " in clientPublicIp:
            clientPublicIp = clientPublicIp.replace(" ", "")
        hostname = socket.gethostname()
        clientLocalIp = socket.gethostbyname(hostname)
    except Exception as e:
        print("Exception while retriving IP Address,using local host IP address", e)
    finally:
        clientPublicIp = "106.193.147.98"
        clientLocalIp = "127.0.0.1"
    clientMacAddress = ':'.join(re.findall('..', '%012x' % uuid.getnode()))
    accept = "application/json"
    userType = "USER"
    sourceID = "WEB"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.Authorization = None
        self.clientLocalIP = self.clientLocalIp
        self.clientPublicIP = self.clientPublicIp
        self.clientMacAddress = self.clientMacAddress
        self.privateKey = api_key
        self.accept = self.accept
        self.userType = self.userType
        self.sourceID = self.sourceID

    async def getHeaders(self):
        return {
            "Content-type": self.accept,
            "X-ClientLocalIP": self.clientLocalIp,
            "X-ClientPublicIP": self.clientPublicIp,
            "X-MACAddress": self.clientMacAddress,
            "Accept": self.accept,
            "X-PrivateKey": self.privateKey,
            "X-UserType": self.userType,
            "X-SourceID": self.sourceID
        }

    async def _request(self, method: str, route: str, rooturl: str, headers: dict = None, auth_token: str = None,
                       parameters: dict = None):
        global data
        if parameters:
            params = parameters.copy()
        else:
            params = {}

        if headers is None:
            headers = await self.getHeaders()

            if auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"

        if method in ["POST", "PUT"]:
            data = json.dumps(params)
        elif method in ["GET", "DELETE"]:
            data = None
            params = params

        url = urljoin(rooturl, AngelConnect._routes[route])

        async with aiohttp.ClientSession() as session:
            async with session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=headers) as response:
                res = await response.json()
        return res

    async def generateLoginsession(self, clientcode: str, passwd: str, secret_key: str):
        totp = pyotp.TOTP(s=secret_key).now()
        params = {
            "clientcode": clientcode,
            "password": passwd,
            "totp": totp
        }
        return await self._request(method="POST", route="api.login", parameters=params, rooturl=AngelConnect._rootUrl)

    async def getProfile(self, auth_token: str):
        return await self._request(method="GET", route="api.user.profile", rooturl=AngelConnect._rootUrl,
                                   auth_token=auth_token)

    async def getOrderBook(self, auth_token: str):
        return await self._request(method="GET",
                                   route="api.order.book",
                                   rooturl=AngelConnect._rootUrl,
                                   auth_token=auth_token)

    async def getLTPData(self, exchange: str, tradingsymbol: str, symboltoken: str, auth_token: str):
        params = {"exchange": exchange,
                  "tradingsymbol": tradingsymbol,
                  "symboltoken": symboltoken}
        return await self._request(method="POST",
                                   route="api.ltp.data",
                                   rooturl=AngelConnect._rootUrl,
                                   parameters=params,
                                   auth_token=auth_token)

    async def placeOrder(self, params: dict, auth_token: str):
        return await self._request(method="POST",
                                   route="api.order.place",
                                   rooturl=AngelConnect._rootUrl,
                                   parameters=params,
                                   auth_token=auth_token)

    async def getRMS(self, auth_token: str):
        return await self._request(method="GET",
                                   route="api.rms.limit",
                                   rooturl=AngelConnect._rootUrl,
                                   auth_token=auth_token)


if __name__ == '__main__':
    print(asyncio.run(AngelConnect('BpCELprq').generateLoginsession("PICI1005", "8040", "BVYCKZ3BRCX5KNGVMOOR56DAUM")))
