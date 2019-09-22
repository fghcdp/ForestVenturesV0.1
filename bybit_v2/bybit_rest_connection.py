import hashlib
import hmac
import json
import logging
import time
import urllib.parse

from requests import Request
from requests import Session
from requests.exceptions import HTTPError


class ByBitRestConnection:
    def __init__(self, api_key, secret, use_test_net):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self._api_key = api_key
        self._secret = secret
        self.url_main = 'https://api.bybit.com'
        self.url_test = 'https://api-testnet.bybit.com'

        if not use_test_net:
            self.url = self.url_main
        else:
            self.url = self.url_test

        self.headers = {'Content-Type': 'application/json'}

        self.s = Session()
        self.s.headers.update(self.headers)

    def change_leverage(self, symbol, leverage="5"):
        leverage = str(leverage)
        payload = {
            'symbol': symbol,
            'leverage': leverage
        }
        return self.__request('POST', '/user/leverage/save', payload=payload)

    def get_positions(self):
        payload = {}
        return self.__request('GET', '/position/list', payload=payload)

    def place_active_order(self, side=None, symbol=None, order_type=None,
                           qty=None, price=None,
                           time_in_force='GoodTillCancel', take_profit=None,
                           stop_loss=None, order_link_id=None):
        payload = {
            'side': side,
            'symbol': symbol,
            'order_type': order_type,
            'qty': qty,
            'price': price,
            'time_in_force': time_in_force,
            'take_profit': take_profit,
            'stop_loss': stop_loss,
            'order_link_id': order_link_id
        }
        return self.__request('POST', '/open-api/order/create', payload=payload)

    def cancel_active_order(self, order_id):
        payload = {
            'order_id': order_id
        }
        return self.__request('POST', '/open-api/order/cancel', payload=payload)

    def __request(self, method, path, payload):
        payload['api_key'] = self._api_key
        payload['timestamp'] = int(time.time() * 1000.0)
        payload['recv_window'] = int(30000)
        payload = dict(sorted(payload.items()))
        for k, v in list(payload.items()):
            if v is None:
                del payload[k]

        param_str = urllib.parse.urlencode(payload)
        sign = hmac.new(self._secret.encode('utf-8'),
                        param_str.encode('utf-8'), hashlib.sha256).hexdigest()
        payload['sign'] = sign

        if method == 'GET':
            query = payload
            body = None
        else:
            query = None
            body = json.dumps(payload)

        req = Request(method, self.url + path, data=body, params=query)
        prepped = self.s.prepare_request(req)

        count = 0
        maxTries = 5
        while True:
            try:
                resp = self.s.send(prepped)
                resp.raise_for_status()
                return resp.json()
            except HTTPError as e:
                if ++count >= maxTries:
                    logging.getLogger(__name__).error('HTTP Connection Error' + str(e))
                    raise e
            except json.decoder.JSONDecodeError as e:
                self.logger.error('json.decoder.JSONDecodeError: ' + str(e))
                raise e
