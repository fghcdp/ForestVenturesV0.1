import logging

from bybit_v2.bybit_rest_connection import ByBitRestConnection
from bybit_v2.bybit_websocket_connection import ByBitWebsocketConnection


class ByBitClient(ByBitRestConnection, ByBitWebsocketConnection):

    def __init__(self, api_key, secret, use_test_net):
        ByBitRestConnection.__init__(self, api_key, secret, use_test_net)
        ByBitWebsocketConnection.__init__(self, api_key, secret, use_test_net)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def run_ws_fetcher(self):
        self._connect_websocket()
