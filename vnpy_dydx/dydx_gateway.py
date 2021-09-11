import hashlib
import hmac
import time
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Sequence

from decimal import Decimal

import pytz
import base64
import json

from requests.exceptions import SSLError
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval,
    Offset
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

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from.dydx_tool import order_to_sign, generate_hash_number


# UTC时区
UTC_TZ = pytz.utc

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
    "PENDING": Status.NOTTRADED,
    "OPEN": Status.NOTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED
}

# 委托类型映射
ORDERTYPE_VT2DYDX: Dict[OrderType, str] = {
    OrderType.LIMIT: "LIMIT",
    OrderType.MARKET: "MARKET"
}
ORDERTYPE_DYDX2VT: Dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2DYDX.items()}

# 买卖方向映射
DIRECTION_VT2DYDX: Dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_DYDX2VT: Dict[str, Direction] = {v: k for k, v in DIRECTION_VT2DYDX.items()}

# 数据频率映射
INTERVAL_VT2DYDX: Dict[Interval, str] = {
    Interval.MINUTE: "1MIN",
    Interval.HOUR: "1HOUR",
    Interval.DAILY: "1DAY",
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


# 账户信息全局缓存字典
api_key_credentials_map: Dict[str, str] = {}


class DydxGateway(BaseGateway):
    """
    vn.py用于对接dYdX交易所的交易接口。
    """

    default_setting: Dict[str, Any] = {
        "key": "",
        "secret": "",
        "passphrase": "",
        "stark_private_key": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
        "limitFee": 0.0,
        "accountNumber": "0"
    }

    exchanges: Exchange = [Exchange.DYDX]

    def __init__(self, event_engine: EventEngine, gateway_name: str = "DYDX") -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.rest_api: "DydxRestApi" = DydxRestApi(self)
        self.ws_api: "DydxWebsocketApi" = DydxWebsocketApi(self)

        self.posid: str = ""
        self.id: str = ""
        self.sys_local_map: Dict[str, str] = {}
        self.local_sys_map: Dict[str, str] = {}

        self.orders: Dict[str, OrderData] = {}

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        api_key_credentials_map["key"] = setting["key"]
        api_key_credentials_map["secret"] = setting["secret"]
        api_key_credentials_map["passphrase"] = setting["passphrase"]
        api_key_credentials_map["stark_private_key"] = setting["stark_private_key"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]
        limitFee: float = setting["limitFee"]
        accountNumber: str = setting["accountNumber"]

        self.rest_api.connect(server, proxy_host, proxy_port, limitFee)
        self.ws_api.connect(proxy_host, proxy_port, server, accountNumber)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.rest_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.ws_api.stop()

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = copy(order)
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0
        self.query_account()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.count: int = 0
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class DydxRestApi(RestClient):
    """dYdX的REST API"""

    def __init__(self, gateway: DydxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.order_count: int = 0

    def sign(self, request: Request) -> Request:
        """生成dYdX签名"""
        security: Security = request.data["security"]
        now_iso_string = generate_now_iso()
        if security == Security.PUBLIC:
            request.data = None
            return request

        else:
            request.data.pop("security")
            signature: str = sign(
                request_path=request.path,
                method=request.method,
                iso_timestamp= now_iso_string,
                data=remove_nones(request.data),
            )
            request.data = json.dumps(remove_nones(request.data))

        headers = {
            "DYDX-SIGNATURE": signature,
            "DYDX-API-KEY": api_key_credentials_map["key"],
            "DYDX-TIMESTAMP": now_iso_string,
            "DYDX-PASSPHRASE": api_key_credentials_map["passphrase"],
            "Accept": 'application/json',
            "Content-Type": 'application/json'
        }
        request.headers = headers

        return request

    def connect(
        self,
        server: str,
        proxy_host: str,
        proxy_port: int,
        limitFee: float
    ) -> None:
        """连接REST服务器"""
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server
        self.limitFee = limitFee

        if self.server == "REAL":
            self.init(REST_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_REST_HOST, proxy_host, proxy_port)

        self.start()
        self.query_contract()

        self.gateway.write_log("REST API启动成功")

    def query_contract(self) -> None:
        """查询合约信息"""
        data: dict = {
            "security": Security.PUBLIC
        }

        self.add_request(
            method="GET",
            path="/v3/markets",
            callback=self.on_query_contract,
            data=data
        )

    def query_account(self) -> None:
        """查询资金"""
        data: dict = {
            "security": Security.PRIVATE
        }

        self.add_request(
            method="GET",
            path=f"/v3/accounts/{self.gateway.id}",
            callback=self.on_query_account,
            data=data
        )

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("%Y%m%d%H%M%S")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(8, "0")

        orderid: str = prefix + suffix
        return orderid

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        # 生成本地委托号
        orderid: str = self.new_orderid()

        # 推送提交中事件
        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        expiration_epoch_seconds: int = int(time.time() + 86400)

        hash_namber: int = generate_hash_number(
                server=self.server,
                position_id=self.gateway.posid,
                client_id=orderid,
                market=req.symbol,
                side=DIRECTION_VT2DYDX[req.direction],
                human_size=str(req.volume),
                human_price=str(req.price),
                limit_fee=str(self.limitFee),
                expiration_epoch_seconds=expiration_epoch_seconds
            )

        signature: str = order_to_sign(hash_namber, api_key_credentials_map["stark_private_key"])

        # 生成委托请求
        data: dict = {
            "security": Security.PRIVATE,
            "market": req.symbol,
            "side": DIRECTION_VT2DYDX[req.direction],
            "type": ORDERTYPE_VT2DYDX[req.type],
            "timeInForce": "GTT",
            "size": str(req.volume),
            "price": str(req.price),
            "limitFee": str(self.limitFee),
            "expiration": epoch_seconds_to_iso(expiration_epoch_seconds),
            "postOnly": False,
            "clientId": orderid,
            "signature": signature
        }

        self.add_request(
            method="POST",
            path="/v3/orders",
            callback=self.on_send_order,
            data=data,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        order_no: str = self.gateway.local_sys_map.get(req.orderid, "")
        if not order_no:
            self.gateway.write_log(f"撤单失败，找不到{req.orderid}对应的系统委托号")
            return

        data: dict = {
            "security": Security.PRIVATE
        }

        order: OrderData = self.gateway.get_order(req.orderid)

        self.add_request(
            method="DELETE",
            path=f"v3/orders/{order_no}",
            callback=self.on_cancel_order,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def query_history(self, req: HistoryRequest) -> List[BarData]:
        """查询历史数据"""
        history: List[BarData] = []
        data: dict = {
            "security": Security.PUBLIC
        }

        params: dict = {
            "resolution": INTERVAL_VT2DYDX[req.interval]
        }

        resp: Response = self.request(
            method="GET",
            path=f"/v3/candles/{req.symbol}",
            data=data,
            params=params
        )

        if resp.status_code // 100 != 2:
            msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
            self.gateway.write_log(msg)

        else:
            data: dict = resp.json()
            if not data:
                self.gateway.write_log("获取历史数据为空")

            for d in data["candles"]:

                bar: BarData = BarData(
                    symbol=req.symbol,
                    exchange=req.exchange,
                    datetime=generate_datetime(d["startedAt"]),
                    interval=req.interval,
                    volume=float(d["baseTokenVolume"]),
                    open_price=float(d["open"]),
                    high_price=float(d["high"]),
                    low_price=float(d["low"]),
                    close_price=float(d["close"]),
                    turnover=float(d["usdVolume"]),
                    open_interest=float(d["startingOpenInterest"]),
                    gateway_name=self.gateway_name
                )
                history.append(bar)

            begin: datetime = history[-1].datetime
            end: datetime = history[0].datetime

            msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
            self.gateway.write_log(msg)

        return history

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data["markets"]:
            contract: ContractData = ContractData(
                symbol=d,
                exchange=Exchange.DYDX,
                name=d,
                pricetick=data["markets"][d]["tickSize"],
                size=data["markets"][d]["stepSize"],
                min_volume=data["markets"][d]["minOrderSize"],
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("合约信息查询成功")

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        d: dict = data["account"]
        balance: float = float(d["equity"])
        available: float = float(d["freeCollateral"])
        account: AccountData = AccountData(
            accountid=d["id"],
            balance=balance,
            frozen=balance-available,
            gateway_name=self.gateway_name
        )
        if account.balance:
            self.gateway.on_account(account)

        for keys in d["openPositions"]:
            position: PositionData = PositionData(
                symbol=keys,
                exchange=Exchange.DYDX,
                direction=Direction.NET,
                volume=float(d["openPositions"][keys]["size"]),
                price=float(d["openPositions"][keys]["entryPrice"]),
                pnl=float(d["openPositions"][keys]["unrealizedPnl"]),
                gateway_name=self.gateway_name
            )
            if d["openPositions"][keys]["size"] == "SHORT":
                position.volume = -position.volume
            self.gateway.on_position(position)

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        pass

    def on_send_order_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """委托下单回报函数报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        if not issubclass(exception_type, (ConnectionError, SSLError)):
            self.on_error(exception_type, exception_value, tb, request)

    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """委托下单失败服务器报错回报"""
        order: OrderData = request.extra
        order.status = Status.REJECTED
        self.gateway.on_order(order)

        msg: str = f"委托失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)

    def on_cancel_order(self, data: dict, request: Request) -> None:
        """委托撤单回报"""
        pass

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """撤单回报函数报错回报"""
        if request.extra:
            order: OrderData = request.extra
            order.status = Status.REJECTED
            self.gateway.on_order(order)

        msg: str = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(msg)


class DydxWebsocketApi(WebsocketClient):
    """dYdX的Websocket API"""

    def __init__(self, gateway: DydxGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: DydxGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.subscribed: Dict[str, SubscribeRequest] = {}
        self.orderbooks: Dict[str, "OrderBook"] = {}

    def connect(
        self,
        proxy_host: str,
        proxy_port: int,
        server: str,
        accountNumber: str
    ) -> None:
        """连接Websocket行情频道"""
        self.accountNumber = accountNumber
        if server == "REAL":
            self.init(WEBSOCKET_HOST, proxy_host, proxy_port)
        else:
            self.init(TESTNET_WEBSOCKET_HOST, proxy_host, proxy_port)
    
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("Websocket API连接成功")
        self.subscribe_topic()

        for req in list(self.subscribed.values()):
            self.subscribe(req)

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("Websocket API连接断开")

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"找不到该合约代码{req.symbol}")
            return

        # 缓存订阅记录
        self.subscribed[req.vt_symbol] = req
        symbol = req.symbol

        orderbook = OrderBook(symbol, req.exchange, self.gateway)
        self.orderbooks[symbol] = orderbook

        req: dict = {
            "type": "subscribe",
            "channel": "v3_orderbook",
            "id": symbol
        }
        self.send_packet(req)

        history_req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=Exchange.DYDX,
            start=None,
            end=None,
            interval=Interval.DAILY
        )
        
        history: List[BarData] = self.gateway.query_history(history_req)

        orderbook.open_price = history[0].open_price
        orderbook.high_price = history[0].high_price
        orderbook.low_price = history[0].low_price
        orderbook.last_price = history[0].close_price

        req: dict = {
            "type": "subscribe",
            "channel": "v3_trades",
            "id": symbol
        }
        self.send_packet(req)

    def subscribe_topic(self) -> None:
        """订阅委托、资金和持仓推送"""
        now_iso_string = generate_now_iso()
        signature: str = sign(
            request_path="/ws/accounts",
            method="GET",
            iso_timestamp= now_iso_string,
            data={},
        )
        req: dict = {
            "type": "subscribe",
            "channel": "v3_accounts",
            "accountNumber": self.accountNumber,
            "apiKey": api_key_credentials_map["key"],
            "signature": signature,
            "timestamp":  now_iso_string,
            "passphrase": api_key_credentials_map["passphrase"]
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        type = packet.get("type", None)
        if type == "error":
            msg: str = packet["message"]
            self.gateway.write_log(msg)
            return

        channel: str = packet.get("channel", None)

        if channel:
            if packet["channel"] == "v3_orderbook" or packet["channel"] == "v3_trades":
                self.on_orderbook(packet)
            elif packet["channel"] == "v3_accounts":
                self.on_message(packet)

    def on_orderbook(self, packet: dict) -> None:
        """订单簿更新推送"""       
        orderbook = self.orderbooks[packet["id"]]
        orderbook.on_message(packet)

    def on_message(self, packet: dict) -> None:
        """Websocket账户更新推送"""
        for order_data in packet["contents"]["orders"]:
            # 绑定本地和系统委托号映射
            self.gateway.local_sys_map[order_data["clientId"]] = order_data["id"]
            self.gateway.sys_local_map[order_data["id"]] = order_data["clientId"]
            order: OrderData = OrderData(
                symbol=order_data["market"],
                exchange=Exchange.DYDX,
                orderid=order_data["clientId"],
                type=ORDERTYPE_DYDX2VT(order_data["type"]),
                direction=DIRECTION_DYDX2VT[order_data["side"]],
                offset=Offset.NONE,
                price=float(order_data["price"]),
                volume=float(order_data["size"]),
                traded=float(order_data["size"]) - float(order_data["remainingSize"]),
                status=STATUS_DYDX2VT.get(order_data["status"], Status.SUBMITTING),
                datetime=generate_datetime(order_data["createdAt"]),
                gateway_name=self.gateway_name
            )
            if 0 < order.traded < order.volume:
                order.status = Status.PARTTRADED
            self.gateway.on_order(order)

        if packet["type"] == "subscribed":
            self.gateway.posid = packet["contents"]["account"]["positionId"]
            self.gateway.id = packet["id"]
            self.gateway.init_query()
            self.gateway.write_log("账户资金查询成功")

        else:
            for fill_data in packet["contents"]["fills"]:
                orderid: str = self.gateway.sys_local_map[fill_data["orderId"]]

                trade: TradeData = TradeData(
                    symbol=fill_data["market"],
                    exchange=Exchange.DYDX,
                    orderid=orderid,
                    tradeid=fill_data["id"],
                    direction=DIRECTION_DYDX2VT[fill_data["side"]],
                    price=float(fill_data["price"]),
                    volume=float(fill_data["size"]),
                    datetime=generate_datetime(fill_data["createdAt"]),
                    gateway_name=self.gateway_name
                )
                self.gateway.on_trade(trade)


class OrderBook():
    """储存dYdX订单簿数据"""

    def __init__(self, symbol: str, exchange: Exchange, gateway: BaseGateway) -> None:
        """构造函数"""

        self.asks: Dict[Decimal, Decimal] = dict()
        self.bids: Dict[Decimal, Decimal] = dict()
        self.gateway: DydxGateway = gateway

        # 创建TICK对象
        self.tick: TickData = TickData(
            symbol=symbol,
            exchange=exchange,
            name=symbol_contract_map[symbol].name,
            datetime=datetime.now(UTC_TZ),
            gateway_name=gateway.gateway_name,
        )

        self.offset: int = 0
        self.open_price: float = 0.0
        self.high_price: float = 0.0
        self.low_price: float = 0.0
        self.last_price: float = 0.0
        self.date: datetime.date = None
        
    def on_message(self, d: dict) -> None:
        """Websocket订单簿更新推送"""
        type: str = d["type"]
        channel: str = d["channel"]
        dt: datetime = datetime.now(UTC_TZ)
        if type == "subscribed" and channel == "v3_orderbook":
            self.on_snapshot(d["contents"]["asks"], d["contents"]["bids"], dt)
        elif type == "channel_data" and channel == "v3_orderbook":
            self.on_update(d["contents"], dt)
        elif channel == "v3_trades":
            self.on_trades(d["contents"]["trades"], dt)

    def on_trades(self, d: list, dt) -> None:
        """成交更新推送"""
        price_list: list = []
        for n in range(len(d)):
            price: float = float(d[n]["price"])
            price_list.append(price)
#            if generate_datetime(d[n]["createdAt"]) != self.date:
#                self.open_price = price

        tick: TickData = self.tick
        tick.high_price = max(self.high_price, max(price_list))
        tick.low_price = min(self.low_price, min(price_list))
        tick.last_price = float(d[0]["price"])
        tick.datetime = generate_datetime(d[0]["createdAt"])

        if not self.date:
            self.date = tick.datetime.date()

        if tick.datetime.date() != self.date:
            req: HistoryRequest = HistoryRequest(
                symbol=tick.symbol,
                exchange=Exchange.DYDX,
                start=None,
                end=None,
                interval=Interval.DAILY
                )
            history: list[BarData] = self.gateway.query_history(req)
            self.open_price = history[0].open_price

        tick.open_price = self.open_price
        tick.localtime = datetime.now()

        self.gateway.on_tick(copy(tick))
        
    def on_update(self, d: dict, dt) -> None:
        """盘口更新推送"""
        offset: int = int(d["offset"])
        if offset < self.offset:
            return
        self.offset = offset
        
        for price, ask_volume in d["asks"]:
            price: float = float(price)
            ask_volume: float = float(ask_volume)
            if price in self.asks:
                if ask_volume > 0:
                    ask_volume: float = float(ask_volume)
                    self.asks[price] = ask_volume
                else:
                    del self.asks[price]
            else:
                if ask_volume > 0:
                    self.asks[price] = ask_volume

        for price, bid_volume in d["bids"]:
            price: float = float(price)
            bid_volume: float = float(bid_volume)
            if price in self.bids:
                if bid_volume > 0:
                    self.bids[price] = bid_volume
                else:
                    del self.bids[price]
            else:
                if bid_volume > 0:
                    self.bids[price] = bid_volume

        self.generate_tick(dt)

    def on_snapshot(self, asks: Sequence[List], bids: Sequence[List], dt: datetime) -> None:
        """盘口推送回报"""
        for n in range(len(asks)):
            price = asks[n]["price"]
            volume = asks[n]["size"]

            self.asks[float(price)] = float(volume)

        for n in range(len(bids)):
            price = bids[n]["price"]
            volume = bids[n]["size"]

            self.bids[float(price)] = float(volume)
 
        self.generate_tick(dt)

    def generate_tick(self, dt: datetime) -> None:
        """合成tick"""
        tick: TickData = self.tick

        bids_keys: list = self.bids.keys()
        bids_keys: list = sorted(bids_keys, reverse=True)

        for i in range(min(5, len(bids_keys))):
            price: float = float(bids_keys[i])
            volume: float = float(self.bids[bids_keys[i]])
            setattr(tick, f"bid_price_{i + 1}", price)
            setattr(tick, f"bid_volume_{i + 1}", volume)

        asks_keys: list = self.asks.keys()
        asks_keys: list = sorted(asks_keys)

        for i in range(min(5, len(asks_keys))):
            price: float = float(asks_keys[i])
            volume: float = float(self.asks[asks_keys[i]])
            setattr(tick, f"ask_price_{i + 1}", price)
            setattr(tick, f"ask_volume_{i + 1}", volume)

        tick.datetime = dt
        tick.localtime = datetime.now()
        self.gateway.on_tick(copy(tick))


def generate_datetime(dt: str) -> datetime:
    """生成时间"""
    dt: datetime = datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S.%fZ')
    dt: datetime = UTC_TZ.localize(dt)
    return dt

def generate_now_iso():
    """生成ISO时间"""
    return datetime.utcnow().strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'

def epoch_seconds_to_iso(epoch):
    """时间格式转换"""
    return datetime.utcfromtimestamp(epoch).strftime(
        '%Y-%m-%dT%H:%M:%S.%f',
    )[:-3] + 'Z'

def sign(
    request_path,
    method,
    iso_timestamp,
    data,
):
    """生成签名"""
    message_string = (
        iso_timestamp +
        method +
        request_path +
        (json_stringify(data) if data else '')
    )
    hashed = hmac.new(
        base64.urlsafe_b64decode(
            (api_key_credentials_map["secret"]).encode('utf-8'),
        ),
        msg=message_string.encode('utf-8'),
        digestmod=hashlib.sha256,
    )
    return base64.urlsafe_b64encode(hashed.digest()).decode()


def json_stringify(data):
    return json.dumps(data, separators=(',', ':'))

def remove_nones(original):
    return {k: v for k, v in original.items() if v is not None}

