import datetime
import hashlib
import hmac
import json
import logging
import threading
import time

import pandas as pd
import websocket
from termcolor import colored
from websocket import WebSocketApp


# noinspection PyMethodMayBeStatic
class ByBitWebsocketConnection:
    BTCUSD_KLINE_1M = 'kline' + '.BTCUSD.' + '1m'
    ETHUSD_KLINE_1M = 'kline' + '.ETHUSD.' + '1m'
    EOSUSD_KLINE_1M = 'kline' + '.EOSUSD.' + '1m'
    XRPUSD_KLINE_1M = 'kline' + '.XRPUSD.' + '1m'
    BTCUSD_ORDERBOOK = "orderBookL2_25." + "BTCUSD"
    ETHUSD_ORDERBOOK = "orderBookL2_25." + "ETHUSD"
    EOSUSD_ORDERBOOK = "orderBookL2_25." + "EOSUSD"
    XRPUSD_ORDERBOOK = "orderBookL2_25." + "XRPUSD"
    POSITION = 'position'
    EXECUTION = 'execution'
    ORDER = 'order'

    # noinspection PyUnresolvedReferences
    def __init__(self, api_key, secret, use_test_net):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self._api_key = api_key
        self._secret = secret
        self.ws_url_main = 'wss://stream.bybit.com/realtime'
        self.ws_url_test = 'wss://stream-testnet.bybit.com/realtime'
        self.__hb_count = 0

        if not use_test_net:
            self.ws_url = self.ws_url_main
        else:
            self.ws_url = self.ws_url_test

        self.ws: WebSocketApp

        self.ws_message_data = {
            self.BTCUSD_KLINE_1M: {},
            self.ETHUSD_KLINE_1M: {},
            self.EOSUSD_KLINE_1M: {},
            self.XRPUSD_KLINE_1M: {},
            self.POSITION: {},
            self.EXECUTION: {},
            self.ORDER: {},
            self.BTCUSD_ORDERBOOK: pd.DataFrame(),
            self.ETHUSD_ORDERBOOK: pd.DataFrame(),
            self.EOSUSD_ORDERBOOK: pd.DataFrame(),
            self.XRPUSD_ORDERBOOK: pd.DataFrame()
        }

        self.__lock = threading.RLock()

    def _connect_websocket(self):
        self.ws = WebSocketApp(url=self.ws_url,
                               on_open=self.__on_open,
                               on_message=self.__on_message,
                               on_close=self.__on_close)

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.wst.join()

        conn_timeout = 10
        while not self.ws.sock or not self.ws.sock.connected and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            raise websocket.WebSocketTimeoutException(
                "Couldn't connect to WS! Exiting. Please check your host settings. ")

    def __on_open(self, ws):
        expires = int(time.time() * 1000) + 10000
        param_str = 'GET/realtime' + str(expires)
        sign = hmac.new(self._secret.encode('utf-8'),
                        param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        ws.send(json.dumps(
            {'op': 'auth', 'args': [self._api_key, expires, sign]}))

        self.ws.send(json.dumps(
            {'op': 'subscribe', 'args': [
                self.BTCUSD_KLINE_1M,
                self.ETHUSD_KLINE_1M,
                self.EOSUSD_KLINE_1M,
                self.XRPUSD_KLINE_1M,
                self.POSITION,
                self.EXECUTION,
                self.ORDER,
                self.BTCUSD_ORDERBOOK,
                self.ETHUSD_ORDERBOOK,
                self.EOSUSD_ORDERBOOK,
                self.XRPUSD_ORDERBOOK
            ]}))

    def __on_close(self, ws):
        raise ConnectionError("Connection Closed Unexpectedly.")

    # noinspection PyUnresolvedReferences
    def __save_orderbook(self, type, message):
        if message['type'] == 'snapshot':
            self.ws_message_data[type] = pd.io.json.json_normalize(message['data']).set_index('id').sort_index(
                ascending=False)
            self._save_orderbook_snapshot(self.ws_message_data[type])
        elif message['type'] == 'delta':
            if len(message['data']['delete']) != 0:
                for x in message['data']['delete']:
                    try:
                        self.ws_message_data[type].drop(index=x['id'])
                    except KeyError:
                        print(colored("%s:INFO: Duplicate orderbookl2_25 delete" % (__name__), "blue"))
                    self._save_orderbook_delta_delete(x)
            if len(message['data']['update']) != 0:
                update_list = pd.io.json.json_normalize(message['data']['update']).set_index('id')
                self.ws_message_data[type].update(update_list)
                self.ws_message_data[type] = self.ws_message_data[type].sort_index(ascending=False)
                for x in message['data']['update']:
                    self._save_orderbook_delta_update(x)
            if len(message['data']['insert']) != 0:
                insert_list = pd.io.json.json_normalize(message['data']['insert']).set_index('id')
                self.ws_message_data[type].update(insert_list)
                self.ws_message_data[type] = self.ws_message_data[type].sort_index(ascending=False)
                for x in message['data']['insert']:
                    self._save_orderbook_delta_insert(x)

    # noinspection PyUnresolvedReferences
    def __on_message(self, ws, message):
        message = json.loads(message)
        if "topic" not in message:
            if message.get("ret_msg") == "pong":
                print(colored("%s:INFO: Heart Beat Message Received at UTC time %s" % (
                    str(__name__), datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')),
                              "magenta"))
            return
        topic = message.get('topic')
        self.__hb_count = self.__hb_count + 1
        if self.__hb_count >= 5:
            ws.send(json.dumps({"op": "ping"}))
            self.__hb_count = 0
        else:
            """we don't need to send the heart beat so frequently. """

        print(colored("%s:INFO: New websocket message for topic %s received at UTC time %s" % (
            str(__name__),
            str(topic),
            datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')), "blue"))

        if topic == self.BTCUSD_KLINE_1M:
            self.ws_message_data[self.BTCUSD_KLINE_1M] = message
        elif topic == self.ETHUSD_KLINE_1M:
            self.ws_message_data[self.ETHUSD_KLINE_1M] = message
        elif topic == self.XRPUSD_KLINE_1M:
            self.ws_message_data[self.XRPUSD_KLINE_1M] = message
        elif topic == self.EOSUSD_KLINE_1M:
            self.ws_message_data[self.EOSUSD_KLINE_1M] = message
        elif topic == self.POSITION:
            self.ws_message_data[self.POSITION] = message
            self._save_position(self.ws_message_data[ByBitWebsocketConnection.POSITION])
        elif topic == self.EXECUTION:
            self.ws_message_data[self.EXECUTION] = message
            self._save_fill_history(self.ws_message_data[ByBitWebsocketConnection.EXECUTION])
        elif topic == self.ORDER:
            self.ws_message_data[self.ORDER] = message
            self._save_order(self.ws_message_data[ByBitWebsocketConnection.ORDER])
        elif topic == self.BTCUSD_ORDERBOOK:
            self.__save_orderbook(self.BTCUSD_ORDERBOOK, message)
        elif topic == self.ETHUSD_ORDERBOOK:
            self.__save_orderbook(self.ETHUSD_ORDERBOOK, message)
        elif topic == self.EOSUSD_ORDERBOOK:
            self.__save_orderbook(self.EOSUSD_ORDERBOOK, message)
        elif topic == self.XRPUSD_ORDERBOOK:
            self.__save_orderbook(self.XRPUSD_ORDERBOOK, message)
