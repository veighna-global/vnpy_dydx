import urllib
import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Any, Dict, List, Tuple
from vnpy.trader.utility import round_to
import pytz

from requests.exceptions import SSLError
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER
from vnpy.event import Event, EventEngine

#from vnpy_rest import Request, RestClient, Response
from vnpy.api.rest import Request, RestClient
from vnpy_okex.okex_gateway import PUBLIC_WEBSOCKET_HOST
from vnpy_websocket import WebsocketClient

# 鉴权类型
class Security(Enum):
    PUBLIC: int = 0
    PRIVATE: int = 1


# 中国时区
CHINA_TZ = pytz.timezone("Asia/Shanghai")

# 实盘REST API地址
REST_HOST: str = "https://api.dydx.exchange"

# 实盘Websocket API地址
WEBSOCKET_HOST: str = "wss://api.dydx.exchange/v3/ws"

# 模拟盘REST API地址
TESTNET_REST_HOST: str = "https://api.stage.dydx.exchange"

# 模拟盘Websocket API地址
TESTNET_WEBSOCKET_HOST: str = "wss://api.stage.dydx.exchange/v3/ws"


# 委托状态映射
STATUS_DYDX2VT: Dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED
}

# 委托类型映射
ORDERTYPE_VT2DYDX: Dict[OrderType, Tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
}
ORDERTYPE_DYDX2VT: Dict[Tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2DYDX.items()}

# 买卖方向映射
DIRECTION_VT2DYDX: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_DYDX2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2DYDX.items()}

# 数据频率映射
INTERVAL_VT2DYDX: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# 时间间隔映射
TIMEDELTA_MAP: Dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# 合约数据全局缓存字典
symbol_contract_map: Dict[str, ContractData] = {}

# 鉴权类型
class Security(Enum):
    PUBLIC: int = 0
    PRIVATE: int = 1


class DydxGateway(BaseGateway):
    """
    vn.py用于对接dYdX交易所的交易接口。
    """

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
    }

    exchanges: Exchange = [Exchange.DYDX]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "DYDX") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        #self.ws_api: "DydxWebsocketApi" = DydxWebsocketApi(self)
        self.rest_api: "DydxRestApi" = DydxRestApi(self)

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["key"]
        secret: str = setting["secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)
        #self.ws_api.connect(proxy_host, proxy_port, server)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        #self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        #self.ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)


class DydxRestApi(RestClient):
    """dYdX的REST API"""

    def __init__(self, gateway: DydxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0
        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.order_count: int = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

    def sign(self, request: Request) -> Request:
        """生成dYdX签名"""
        security: Security = request.data["security"]
        if security == Security.PUBLIC:
            request.data = None
            return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.query_contract()

        self.gateway.write_log("REST API启动成功")

    def query_time(self) -> None:
        """查询时间"""
        pass

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_order(self) -> None:
        """查询未成交委托"""
        pass

    def query_contract(self) -> None:
        """查询合约信息"""
        data: dict = {
            "security": Security.PUBLIC
        }

        path: str = "/v3/markets"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def _new_order_id(self) -> int:
        """生成本地委托号"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        pass

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        pass

    def on_query_time(self, data: dict, request: Request) -> None:
        """时间查询回报"""
        pass

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        pass

    def on_query_order(self, data: dict, request: Request) -> None:
        """未成交委托查询回报"""
        pass

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        print(data)

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        pass


class DydxWebsocketApi(WebsocketClient):
    """币安正向合约的行情Websocket API"""

    def __init__(self, gateway: DydxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.ticks: Dict[str, TickData] = {}
        self.reqid: int = 0

    def connect(
        self,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        """连接Websocket行情频道"""
        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port)
    
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("行情Websocket API连接刷新")

        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        pass

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        pass


def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = CHINA_TZ.localize(dt)
    return dt

def generate_query_path(url, params):
    entries = params.items()
    if not entries:
        return url

    paramsString = '&'.join('{key}={value}'.format(
        key=x[0], value=x[1]) for x in entries if x[1] is not None)
    if paramsString:
        return url + '?' + paramsString

    return url