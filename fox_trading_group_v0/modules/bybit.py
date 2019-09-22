import time

import pymysql
from termcolor import colored

from burning_house_v1.battlefield_situation import BattleFieldSituation
from burning_house_v1.burning_house_strategy import BurningHouseStrategy
from bybit_v2.bybit_client import ByBitClient
from fox_trading_group_v0.config import *
from utils.utils import bcolors


class Bybit:
    BTCUSD_LEVERAGE = 20
    ETHUSD_LEVERAGE = 20
    EOSUSD_LEVERAGE = 20
    XRPUSD_LEVERAGE = 20

    bhs_btc = {}
    bhs_eth = {}
    bhs_eos = {}
    bhs_xrp = {}

    def __init__(self, rdbhost, rdbuser, rdbpassword, rdbdatabase, rdbcharset, bybit_client: ByBitClient):
        self.rdbcharset = rdbcharset
        self.rdbdatabase = rdbdatabase
        self.rdbpassword = rdbpassword
        self.rdbuser = rdbuser
        self.rdbhost = rdbhost
        self.connection = pymysql.connect(host=rdbhost,
                                          user=rdbuser,
                                          password=rdbpassword,
                                          db=rdbdatabase,
                                          charset=rdbcharset,
                                          cursorclass=pymysql.cursors.DictCursor)
        self.connection.autocommit(True)
        self.global_reading_cursor = self.connection.cursor()
        self.bybit_client = bybit_client
        self.latest_800_klines_sql = 'SELECT `open`, `high`, `low`, `close`, `timestamp`, `interval`, `volume` FROM `klines` WHERE `symbol` LIKE %s ORDER BY `timestamp` DESC LIMIT 800'
        self.__init_rdb()
        self.__ensure_leverage()

    def __init_rdb(self):
        sql = """
        DROP TABLE IF EXISTS `battlefield_situation_history`
        """
        self.connection.cursor().execute(sql)
        self.connection.commit()
        sql = """
CREATE TABLE `battlefield_situation_history` (
  `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `average_volume` BIGINT(20) NOT NULL ,
  `timestamp` BIGINT(30) NOT NULL ,
  `latest_upper_band_price` DECIMAL(40,20) NOT NULL ,
  `linear_regression_line_price` DECIMAL(40, 20) NOT NULL ,
  `latest_lower_band_price` DECIMAL(40, 20) NOT NULL ,
  `linear_regression_correlation_coefficient` DECIMAL(40, 20) NOT NULL ,
  `linear_regression_pearson_value` DECIMAL(40, 20) NOT NULL ,
  `linear_regression_up_band_height` DECIMAL(40, 20) NOT NULL ,
  `linear_regression_dn_band_height` DECIMAL(40, 20) NOT NULL ,
  `battle_field_situation` VARCHAR(40) NOT NULL
)
  ENGINE = InnoDB;
        """
        self.connection.cursor().execute(sql)
        self.connection.commit()

    def warm_up_finished(self):
        self.global_reading_cursor.execute(self.latest_800_klines_sql, (BYBIT_BTC))
        latest_800_klines = self.global_reading_cursor.fetchall()
        if len(latest_800_klines) != 800:
            print(bcolors.WARNING + '%s:INFO: waiting bybit client to warm up the database, progress: %s%% :' % (
                __name__, float(len(latest_800_klines)) / 800.0 * 100.0) + bcolors.ENDC)
            return False
        else:
            return True

    def analyze_bybit_tickers(self):
        self.bhs_btc = self.__analyze_ticker(BYBIT_BTC)
        self.bhs_eth = self.__analyze_ticker(BYBIT_ETH)
        self.bhs_eos = self.__analyze_ticker(BYBIT_EOS)
        self.bhs_xrp = self.__analyze_ticker(BYBIT_XRP)

    def report_status(self):
        self.__report_status(BYBIT_BTC)
        self.__report_status(BYBIT_ETH)
        self.__report_status(BYBIT_EOS)
        self.__report_status(BYBIT_XRP)

    def __report_status(self, symbol):
        if symbol == BYBIT_BTC:
            self.__save_status_to_db(self.bhs_btc)
        elif symbol == BYBIT_ETH:
            self.__save_status_to_db(self.bhs_eth)
        elif symbol == BYBIT_EOS:
            self.__save_status_to_db(self.bhs_eos)
        elif symbol == BYBIT_XRP:
            self.__save_status_to_db(self.bhs_xrp)
        else:
            raise ValueError("%s:ERROR: Invalid Symbol" % (__name__))

    def __save_status_to_db(self, bhs):
        sql = 'INSERT INTO `battlefield_situation_history` (average_volume, timestamp, latest_upper_band_price, linear_regression_line_price, latest_lower_band_price, linear_regression_correlation_coefficient, linear_regression_pearson_value, linear_regression_up_band_height, linear_regression_dn_band_height, battle_field_situation) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        _average_volume = bhs.average_volume()
        _timestamp = bhs.latest_timestamp()
        _latest_upper_band_price = bhs.latest_upper_band_price()
        _linear_regression_line_price = bhs.latest_regression_line_price()
        _latest_lower_band_price = bhs.latest_lower_band_price()
        _linear_regression_correlation_coefficient = bhs.regression_correlation_coefficient()
        _linear_regression_pearson_value = bhs.regression_pearson_value()
        _linear_regression_up_band_height = bhs.regression_upper_band_height()
        _linear_regression_dn_band_height = bhs.regression_lower_band_height()
        _battle_field_situation = bhs.battlefield_situtation()
        c = self.connection.cursor()
        c.execute(sql, (_average_volume, _timestamp, _latest_upper_band_price, _linear_regression_line_price,
                        _latest_lower_band_price, _linear_regression_correlation_coefficient,
                        _linear_regression_pearson_value, _linear_regression_up_band_height,
                        _linear_regression_dn_band_height, _battle_field_situation))
        self.connection.commit()

    def __analyze_ticker(self, symbol):
        self.global_reading_cursor.execute(self.latest_800_klines_sql, (symbol))
        latest_800_klines = self.global_reading_cursor.fetchall()
        for i in latest_800_klines:
            i["high"] = float(i["high"])
            i["low"] = float(i["low"])
            i["close"] = float(i["close"])
            i["open"] = float(i["open"])
        bhs = BurningHouseStrategy(latest_800_klines, "1m", symbol)
        return bhs

    def is_data_stream_alive(self) -> bool:
        sql = 'select `timestamp` from klines order by `timestamp` DESC LIMIT 1'
        c = self.connection.cursor()
        c.execute(sql)
        rd = c.fetchone()
        if rd is not None and time.time() <= int(rd["timestamp"]) + 60:
            return True
        else:
            # TODO: test database
            if rdbdatabase == "test_bybit_testnet_bot":
                return True
            else:
                return False

    # TODO: Trade Tickers with REST API
    def trade_ticker(self) -> None:
        if self.bhs_btc.battlefield_situtation() == BattleFieldSituation.LONGENTRY:
            long_entry_price = self.__orderbook_buy_price(BYBIT_BTC, 3)
            long_entry_contracts = self.__max_available_entry_contracts(BYBIT_BTC, self.BTCUSD_LEVERAGE,
                                                                        long_entry_price)
            if long_entry_contracts <= 1: return
        elif self.bhs_btc.battlefield_situtation() == BattleFieldSituation.SHORTENTRY:
            short_entry_price = self.__orderbook_sell_price(BYBIT_BTC, 3)
            short_entry_contracts = self.__max_available_entry_contracts(BYBIT_BTC, self.BTCUSD_LEVERAGE,
                                                                         short_entry_price)
            if short_entry_contracts <= 1: return
        elif self.bhs_btc.battlefield_situtation() == BattleFieldSituation.LONGEXIT:
            long_exit_price = self.__orderbook_sell_price(BYBIT_BTC, 2)
            long_exit_contracts = self.__max_available_exit_contracts(BYBIT_BTC)
            self.__place_limit_reduce_only_order(long_exit_price, long_exit_contracts)

        elif self.bhs_btc.battlefield_situtation() == BattleFieldSituation.SHORTEXIT:
            short_exit_price = self.__orderbook_buy_price(BYBIT_BTC, 2)
            short_exit_contracts = self.__max_available_exit_contracts(BYBIT_BTC)
            self.__place_limit_reduce_only_order(short_exit_price, short_exit_contracts)

    # TODO: Core HFT
    def __place_limit_reduce_only_order(self, exit_price, exit_contracts):
        pass

    def __place_limit_post_only_order(self, entry_price, entry_contracts):
        pass

    def __max_available_entry_contracts(self, symbol, leverage, entry_price) -> int:
        pass

    def __max_available_exit_contracts(self, symbol) -> int:
        pass

    def __orderbook_buy_price(self, symbol, depth: int) -> float:
        # TODO: PROD CHECK DESC
        sql_1 = 'select `price` from `orderbookl2` where `symbol` like %s and side like "Buy" order by `price` DESC '
        sql_2 = 'limit %d, 1' % (int(depth - 1))
        if not 1 <= depth <= 25:
            raise ValueError("%s:ERROR: invalid depth %d:" % (__name__, depth))
        sql = sql_1 + sql_2
        c = self.connection.cursor()
        c.execute(sql, (symbol))
        c.fetchone()
        if c is None:
            raise RuntimeError("%s:ERROR: orderbook buy side query returns None:" % (__name__))
        else:
            return float(c["price"])

    def __orderbook_sell_price(self, symbol, depth: int) -> float:
        # TODO: PROD CHECK ASC
        sql_1 = 'select `price` from `orderbookl2` where `symbol` like %s and side like "Sell" order by `price` ASC '
        sql_2 = 'limit %d, 1' % (int(depth - 1))
        if not 1 <= depth <= 25:
            raise ValueError("%s:ERROR: invalid depth %d:" % (__name__, depth))
        sql = sql_1 + sql_2
        c = self.connection.cursor()
        c.execute(sql, (symbol))
        c.fetchone()
        if c is None:
            raise RuntimeError("%s:ERROR: orderbook buy side query returns None:" % (__name__))
        else:
            return float(c["price"])

    def __ensure_leverage(self):
        self.bybit_client.change_leverage(BYBIT_BTC, 1).get("ret_code")
        self.bybit_client.change_leverage(BYBIT_ETH, 1).get("ret_code")
        self.bybit_client.change_leverage(BYBIT_EOS, 1).get("ret_code")
        self.bybit_client.change_leverage(BYBIT_XRP, 1).get("ret_code")
        if self.bybit_client.change_leverage(BYBIT_BTC, self.BTCUSD_LEVERAGE).get("ret_code") != 0 or \
                self.bybit_client.change_leverage(BYBIT_ETH, self.ETHUSD_LEVERAGE).get("ret_code") != 0 or \
                self.bybit_client.change_leverage(BYBIT_EOS, self.EOSUSD_LEVERAGE).get("ret_code") != 0 or \
                self.bybit_client.change_leverage(BYBIT_XRP, self.XRPUSD_LEVERAGE).get("ret_code") != 0:
            raise ValueError("%s:ERROR: ensure leverage does not return 0 " % (__name__))
        else:
            print(colored("%s:INFO: leverage for BTC is %d" % (__name__, self.BTCUSD_LEVERAGE), "yellow"))
            print(colored("%s:INFO: leverage for ETH is %d" % (__name__, self.ETHUSD_LEVERAGE), "yellow"))
            print(colored("%s:INFO: leverage for EOS is %d" % (__name__, self.EOSUSD_LEVERAGE), "yellow"))
            print(colored("%s:INFO: leverage for XRP is %d" % (__name__, self.XRPUSD_LEVERAGE), "yellow"))
