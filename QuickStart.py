import threading
import traceback

from termcolor import colored

from bybit_v2.bybit_klines_fetcher import ByBitKlinesFetcher
from fox_trading_group_v0.fox_trading_group import FoxTradingGroup


# noinspection PyMethodMayBeStatic,SpellCheckingInspection
class QuickStart:
    rdbhost = "127.0.0.1"
    rdbuser = "bybitbot"
    rdbpassword = "bybitbotpass"
    rdbdatabase = "bybit_testnet_bot"
    rdbcharset = "utf8"

    bybit_client = {}

    sync_thread = {}
    trader_thread = {}

    def init_threads(self):
        self.bybit_client, self.sync_thread = self.bybit_fetcher()
        self.trader_thread = self.fox_trading_group(self.bybit_client)

    def bybit_fetcher(self):
        bybit_client = ByBitKlinesFetcher(api_key='**********',
                                          secret='*********',
                                          use_test_net=True,
                                          save_to_rdb=True,
                                          rdbhost=self.rdbhost,
                                          rdbuser=self.rdbuser,
                                          rdbpassword=self.rdbpassword,
                                          rdbdatabase=self.rdbdatabase,
                                          rdbcharset=self.rdbcharset)
        return bybit_client, threading.Thread(target=lambda: bybit_client.run_ws_fetcher(), daemon=True)

    def fox_trading_group(self, bybit_client):
        ftg = FoxTradingGroup(bybit_client)
        return threading.Thread(target=lambda: ftg.start_trading(), daemon=True)

    def run_forever(self):
        while True:
            try:
                self.init_threads()
                self.sync_thread.start()
                self.trader_thread.start()
                self.trader_thread.join()
            except Exception as err:
                traceback.print_exc()
                traceback.print_stack()
                print(colored("%s:ERROR:run forever function raised an exception that: %s:" % (__name__, str(err)),
                              "red"))
                print(colored("%s:ERROR: clear database and restarting" % (__name__), "red"))


if __name__ == '__main__':
    bybit_bot = QuickStart()
    bybit_bot.run_forever()
