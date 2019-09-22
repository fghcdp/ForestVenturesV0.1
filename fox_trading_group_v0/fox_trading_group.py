from termcolor import colored

from fox_trading_group_v0.config import *
from fox_trading_group_v0.ftg.ftg_base import FTGBase
from fox_trading_group_v0.modules.bybit import Bybit


class FoxTradingGroup(FTGBase):
    def __init__(self, bybit_client):
        self.bybit = Bybit(rdbhost, rdbuser, rdbpassword, rdbdatabase, rdbcharset, bybit_client)
        self._trade_round = 0

    def _assert_data_stream_alive(self):
        if not self.bybit.is_data_stream_alive():
            print(colored("%s:ERROR: bybit websocket datastream is down, exit code: 2 : " % (__name__), "red"))
            exit(2)

    def _analyze(self):
        self.bybit.analyze_bybit_tickers()

    def _trade(self):
        self.bybit.trade_ticker()

    def _report(self, second_elapsed):
        print("%s:INFO: trade round %s elapsed %s seconds:" % (__name__, self._trade_round, second_elapsed))
        self.bybit.report_status()
