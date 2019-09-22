import datetime
import logging
import threading
import time
import traceback

import pymysql

from bybit_v2.bybit_client import ByBitClient
from bybit_v2.bybit_websocket_connection import ByBitWebsocketConnection


# noinspection PyBroadException
class ByBitKlinesFetcher(ByBitClient):
    inited = False

    # noinspection SpellCheckingInspection
    def __init__(self, api_key, secret, use_test_net, save_to_rdb, rdbhost, rdbuser, rdbpassword,
                 rdbdatabase, rdbcharset):
        ByBitClient.__init__(self, api_key, secret, use_test_net)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if save_to_rdb:
            self.rdbhost = rdbhost
            self.rdbuser = rdbuser
            self.rdbpassword = rdbpassword
            self.rdbdatabase = rdbdatabase
            self.rdbcharset = rdbcharset
            self.cursorclass = pymysql.cursors.DictCursor
            if not self.inited:
                self.__init_rdb()
                self.kline_fetcher_rdb_connection = self._new_rdb_connection()
                threading.Thread(target=lambda: self.__save_klines_run_forever()).start()
                self.inited = True

    def __save_klines_run_forever(self):
        self._save_kline_rdb_connection = self._new_rdb_connection()
        while True:
            try:
                self._save_kline(self.ws_message_data[ByBitWebsocketConnection.BTCUSD_KLINE_1M])
                self._save_kline(self.ws_message_data[ByBitWebsocketConnection.ETHUSD_KLINE_1M])
                self._save_kline(self.ws_message_data[ByBitWebsocketConnection.EOSUSD_KLINE_1M])
                self._save_kline(self.ws_message_data[ByBitWebsocketConnection.XRPUSD_KLINE_1M])
                time.sleep(0.25)
            except Exception as err:
                traceback.print_exc()
                traceback.print_stack()

    def _new_rdb_connection(self):
        return pymysql.connect(host=self.rdbhost,
                               user=self.rdbuser,
                               password=self.rdbpassword,
                               db=self.rdbdatabase,
                               charset=self.rdbcharset,
                               port=3306,
                               cursorclass=pymysql.cursors.DictCursor)

    def __init_rdb(self):
        connection = self._new_rdb_connection()
        connection.cursor().execute("DROP TABLE IF EXISTS klines")
        connection.commit()
        connection.cursor().execute("""
CREATE TABLE klines (
  `id`        BIGINT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `symbol`    VARCHAR(20)     NOT NULL,
  `open_time` BIGINT(20)      NOT NULL,
  `open`      DECIMAL(40, 20) NOT NULL,
  `high`      DECIMAL(40, 20) NOT NULL,
  `low`       DECIMAL(40, 20) NOT NULL,
  `close`     DECIMAL(40, 20) NOT NULL,
  `volume`    BIGINT(30)      NOT NULL,
  `turnover`  BIGINT(30)      NOT NULL,
  `interval`  VARCHAR(10)     NOT NULL,
  `timestamp` BIGINT(20)      NOT NULL,
  `datetime`  VARCHAR(40)     NOT NULL,
  UNIQUE KEY `idx_symbol_at_timestamp_in_interval` (`symbol`, `interval`, `timestamp`)
)
  ENGINE = InnoDB;
        """)
        connection.commit()
        connection.cursor().execute("DROP TABLE IF EXISTS `fill_history`")
        connection.commit()
        connection.cursor().execute(
            """
CREATE TABLE `fill_history` (
  `id`            BIGINT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `symbol`        VARCHAR(12)     NOT NULL,
  `side`          VARCHAR(8)      NOT NULL,
  `order_id`      VARCHAR(72)     NOT NULL,
  `exec_id`       VARCHAR(72)     NOT NULL,
  `order_link_id` VARCHAR(36)     NOT NULL,
  `price`         DECIMAL(40, 20) NOT NULL,
  `exec_qty`      BIGINT(20)      NOT NULL,
  `exec_fee`      DECIMAL(40, 20) NOT NULL,
  `leaves_qty`    BIGINT(20)      NOT NULL,
  `is_maker`      TINYINT(2)      NOT NULL,
  `trade_time`    VARCHAR(48)     NOT NULL,
  UNIQUE `idx_execution_on_order`(`order_id`, `exec_id`)
)
  ENGINE = InnoDB;
            """
        )
        connection.commit()
        connection.cursor().execute("DROP TABLE IF EXISTS `positions`")
        connection.commit()
        connection.cursor().execute(
            """
 CREATE TABLE `positions` (
  `id` BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `symbol` VARCHAR(12) NOT NULL ,
  `size` BIGINT(20) NOT NULL ,
  `entry_price` DECIMAL(40, 20) NOT NULL ,
  `liq_price` DECIMAL(40, 20) NOT NULL ,
  `bust_price` DECIMAL(40, 20) NOT NULL,
  `take_profit` DECIMAL(40, 20) NOT NULL ,
  `stop_loss` DECIMAL(40, 20) NOT NULL ,
  `trailing_stop` DECIMAL(40, 20) NOT NULL ,
  `position_value` DECIMAL(40, 20),
  `auto_add_margin` TINYINT(2) NOT NULL ,
  `position_seq` INT(5) NOT NULL ,
  UNIQUE `idx_position_seq` (`position_seq`)
)
  ENGINE = InnoDB;
            """
        )
        connection.commit()
        connection.cursor().execute("DROP TABLE IF EXISTS `orders`")
        connection.commit()
        connection.cursor().execute(
            """
CREATE TABLE `orders` (
  `id`            INT             NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `order_id`      VARCHAR(72)     NOT NULL,
  `order_link_id` VARCHAR(16)     NOT NULL,
  `side`          VARCHAR(8)      NOT NULL,
  `order_type`    VARCHAR(12)     NOT NULL,
  `price`         DECIMAL(40, 20) NOT NULL,
  `qty`           BIGINT(40)      NOT NULL,
  `time_in_force` VARCHAR(28)     NOT NULL,
  `order_status`  VARCHAR(18)     NOT NULL,
  `leaves_qty`    BIGINT(40)      NOT NULL,
  `cum_exec_qty`  BIGINT(40)      NOT NULL,
  `cum_exec_fee`  BIGINT(40)      NOT NULL,
  `timestamp`     VARCHAR(48)     NOT NULL,
  UNIQUE `idx_order_id_order_link_id` (`order_id`, `order_link_id`)
)
  ENGINE = InnoDB;
            """
        )
        connection.commit()
        connection.cursor().execute("DROP TABLE IF EXISTS `orderbookl2`")
        connection.commit()
        connection.cursor().execute(
            """
CREATE TABLE `orderbookl2` (
  `orderbookL2_id` BIGINT(50)      NOT NULL,
  `symbol`         VARCHAR(12)     NOT NULL,
  `price`          DECIMAL(40, 20) NOT NULL,
  `side`           VARCHAR(8)      NOT NULL,
  `size`           BIGINT(20)      NOT NULL,
  INDEX `idx_price` (`price`),
  UNIQUE `idx_orderbook_id` (`orderbookL2_id`)
)
  ENGINE = InnoDB;
            """
        )
        connection.commit()
        connection.close()

    def _save_orderbook_snapshot(self, orderbook, connection):
        connection.cursor().execute(
            'DELETE FROM orderbookl2 where symbol like "%s"' % ((orderbook["symbol"]).iloc[0]))
        connection.commit()
        for index, row in orderbook.iterrows():
            __orderbookL2_id = str(index)
            __symbol = row["symbol"]
            __price = row["price"]
            __side = row["side"]
            __size = row["size"]
            sql = "INSERT INTO `orderbookL2` (orderbookL2_id, symbol, price, side, size) VALUE (%s, %s, %s, %s, %s)"
            connection.cursor().execute(sql, (__orderbookL2_id, __symbol, __price, __side, __size))
            connection.commit()
        connection.close()

    def _save_orderbook_delta_delete(self, delete_item, connection):
        __price = delete_item["price"]
        __id = str(delete_item["id"])
        __symbol = delete_item["symbol"]
        __side = delete_item["side"]
        sql = 'DELETE FROM `orderbookl2` WHERE orderbookL2_id LIKE %s'
        connection.cursor().execute(sql, (__id))
        connection.close()

    def _save_orderbook_delta_update(self, update_item, connection):
        __price = update_item["price"]
        __id = str(update_item["id"])
        __symbol = update_item["symbol"]
        __side = update_item["side"]
        __size = update_item["size"]

        sql = 'UPDATE `orderbookl2` SET `price` = %s, `side` = %s, `size`=%s WHERE `orderbookL2_id` = %s AND `symbol` = %s'
        connection.cursor().execute(sql, (__price, __side, __size, __id, __symbol))
        connection.commit()
        connection.close()

    def _save_orderbook_delta_insert(self, insert_item, connection):
        __orderbookL2_id = insert_item["id"]
        __symbol = insert_item["symbol"]
        __price = insert_item["price"]
        __side = insert_item["side"]
        __size = insert_item["size"]
        sql = "REPLACE INTO `orderbookL2` (orderbookL2_id, symbol, price, side, size) VALUE (%s, %s, %s, %s, %s)"
        connection.cursor().execute(sql, (__orderbookL2_id, __symbol, __price, __side, __size))
        connection.commit()
        connection.close()

    def _save_order(self, order, connection):
        """
        :param order:
        // Response content format
        {
            "topic":"order",
            "data":[
                {
                    "order_id":"xxxxxxxx-xxxx-xxxx-832b-1eca710bf0a6",
                    "order_link_id":"xxxxxxxx",
                    "symbol":"BTCUSD",
                    "side":"Sell",
                    "order_type":"Limit",
                    "price":3559.5,
                    "qty":850,
                    "time_in_force":"GoodTillCancel",
                    "order_status":"Cancelled",
                    "leaves_qty":0,
                    "cum_exec_qty":0,
                    "cum_exec_value":0,
                    "cum_exec_fee":0,
                    "timestamp":"2019-01-22T14:49:38.000Z"
                }
            ]
        }
        """
        if "data" not in order:
            self.logger.info("INFO: Position Message Preparing ... ")
            return
        __d = order["data"]
        for d in __d:
            __order_id = d["order_id"]
            __order_link_id = d["order_link_id"]
            __side = d["side"]
            __order_type = d["order_type"]
            __price = d["price"]
            __qty = d["qty"]
            __time_in_force = d["time_in_force"]
            __order_status = d["order_status"]
            __leaves_qty = d["leaves_qty"]
            __cum_exec_qty = d["cum_exec_qty"]
            __cum_exec_fee = d["cum_exec_fee"]
            __timestamp = d["timestamp"]

            sql = "REPLACE INTO `orders` (order_id, order_link_id, side, order_type, price, qty, time_in_force, order_status, leaves_qty, cum_exec_qty, cum_exec_fee, timestamp) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            connection.cursor().execute(sql, (
                __order_id, __order_link_id, __side, __order_type, __price, __qty, __time_in_force, __order_status,
                __leaves_qty, __cum_exec_qty, __cum_exec_fee, __timestamp))
            connection.commit()
        connection.close()

    def _save_position(self, position, connection):
        """
        :param position:
        // Response content format
        {
           "topic":"position",
           "action":"update",
           "data":[
               {
                   "symbol":"BTCUSD",                  // the contract for this position
                   "side":"Sell",                      // side
                   "size":11,                          // the current position amount
                   "entry_price":6907.291588174717,    // entry price
                   "liq_price":7100.234,               // liquidation price
                   "bust_price":7088.1234,             // bankruptcy price
                   "take_profit":0,                    // take profit price
                   "stop_loss":0,                      // stop loss price
                   "trailing_stop":0,                  // trailing stop points
                   "position_value":0.00159252,        // positional value
                   "leverage":1,                       // leverage
                   "position_status":"Normal",         // status of position (Normal:normal Liq:in the process of liquidation Adl:in the process of Auto-Deleveraging)
                   "auto_add_margin":0,                // Auto margin replenishment enabled (0:no 1:yes)
                   "position_seq":14                   // position version number
               }
           ]
        }
        """
        if "data" not in position:
            self.logger.info("INFO: Position Message Preparing ... ")
            return
        __d = position["data"]
        for d in __d:
            __symbol = d["symbol"]
            __size = d["size"]
            __entry_price = d["entry_price"]
            __liq_price = d["liq_price"]
            __bust_price = d["bust_price"]
            __take_profit = d["take_profit"]
            __stop_loss = d["stop_loss"]
            __trailing_stop = d["trailing_stop"]
            __position_value = d["position_value"]
            __auto_add_margin = d["auto_add_margin"]
            __position_seq = d["position_seq"]

            sql = "REPLACE INTO positions (symbol, size, entry_price, liq_price, bust_price, take_profit, stop_loss, trailing_stop, position_value, auto_add_margin, position_seq) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            connection.cursor().execute(sql, (
                __symbol, __size, __entry_price, __liq_price, __bust_price, __take_profit, __stop_loss, __trailing_stop,
                __position_value, __auto_add_margin, __position_seq))
            connection.commit()
        connection.close()

    def _save_fill_history(self, execution, connection):
        """
        :param execution:
        {
            "topic":"execution",
            "data":[
                {
                    "symbol":"BTCUSD",
                    "side":"Sell",
                    "order_id":"xxxxxxxx-xxxx-xxxx-9a8f-4a973eb5c418",
                    "exec_id":"xxxxxxxx-xxxx-xxxx-8b66-c3d2fcd352f6",
                    "order_link_id":"xxxxxxx",
                    "price":3559,
                    "exec_qty":1028,
                    "exec_fee":-0.00007221,
                    "leaves_qty":0,
                    "is_maker":true,
                    "trade_time":"2019-01-22T14:49:38.000Z"
                },
            ]
        }
        """
        if "data" not in execution:
            self.logger.info("INFO: Execution Message Preparing ... ")
            return
        __d = execution["data"]
        for d in __d:
            __symbol = d["symbol"]
            __side = d["side"]
            __order_id = d["order_id"]
            __exec_id = d["exec_id"]
            __order_link_id = d["order_link_id"]
            __price = d["price"]
            __exec_qty = d["exec_qty"]
            __exec_fee = d["exec_fee"]
            __leaves_qty = d["leaves_qty"]
            __is_maker = d["is_maker"]
            __trade_time = d["trade_time"]

            sql = "REPLACE INTO fill_history (symbol, side, order_id, exec_id, order_link_id, price, exec_qty, exec_fee, leaves_qty, is_maker, trade_time) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            connection.cursor().execute(sql, (
                __symbol, __side, __order_id, __exec_id, __order_link_id, __price, __exec_qty, __exec_fee, __leaves_qty,
                __is_maker, __trade_time))
            connection.commit()
        connection.close()

    def _save_kline(self, kline_message):
        """
        Save Kline into Database
        Use [] instead of .get()
        :param ticker:
        {
            "topic": "kline.BTCUSD.1m",
            "data": {
                "id": 0,
                "symbol": "BTCUSD",
                "open_time": 1568983740,
                "open": 10170,
                "high": 10170,
                "low": 10170,
                "close": 10170,
                "volume": 0,
                "turnover": 0,
                "interval": "1m",
                "timestamp": 1568983749
            }
        }
        """
        connection = self._save_kline_rdb_connection
        if "data" not in kline_message:
            self.logger.info("INFO: Kline Message Preparing ... ")
            return
        d = kline_message["data"]
        _symbol = d["symbol"]
        _open_time = d["open_time"]
        _open = d["open"]
        _high = d["high"]
        _low = d["low"]
        _close = d["close"]
        _volume = d["volume"]
        _turnover = d["turnover"]
        _interval = d["interval"]
        _timestamp = time.time()
        _datetime = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

        sql = "REPLACE INTO klines (symbol, open_time, open, high, low, close, volume, turnover, `interval`, timestamp, datetime) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        connection.cursor().execute(sql, (
            _symbol, _open_time, _open, _high, _low, _close, _volume, _turnover, _interval, _timestamp, _datetime))
        connection.commit()