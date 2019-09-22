from copy import deepcopy

import numpy as np


class TradingStrategy(object):
    def __init__(self, klines_list, interval, symbol):
        self._interval = interval
        self._symbol = symbol
        self._klines = self.__check_kline_list(klines_list)

    def __str__(self):
        print("%s:INFO: average volume is %s for symbol %s:" % (__name__, self.average_volume(), self._symbol))
        print("%s:INFO:latest high is %s for symbol %s:" % (__name__, self.latest_high(), self._symbol))
        print("%s:INFO:latest low is %s for symbol %s:" % (__name__, self.latest_low(), self._symbol))
        print("%s:INFO:latest open is %s for symbol %s:" % (__name__, self.latest_open(), self._symbol))
        print("%s:INFO:latest close is %s for symbol %s:" % (__name__, self.latest_close(), self._symbol))
        print("%s:INFO:latest volume is %s for symbol %s:" % (__name__, self.latest_volume(), self._symbol))
        print("%s:INFO:latest timestamp is %s for symbol %s:" % (__name__, self.latest_timestamp(), self._symbol))
        print("%s:INFO:latest volume greater than average is %s for symbol %s:" % (
            __name__, self.latest_volume_greater_than_average(), self._symbol))

    @staticmethod
    def __check_kline_list(klines_list):
        """
        :param klines_list:
        [{
            "open": 10293.0
            "close": 15680.0,
            "high": 15690.0,
            "low": 15670.0,
            "volume": "3",
            "timestamp": 1569041656,
            "interval": "1m"
        }]
        :exception: ValueError:
        WARNING: **WE DO NOT CHECK WHETHER THE LIST IS SORTED :) It's your duty to sort the list in ASC order**
        WELL :) just a joke, we sort it for you base on timestamp
        :return: {list} klines_list
        """
        klines_list = sorted(deepcopy(klines_list), key=lambda k: k["timestamp"])
        if not isinstance(klines_list, list):
            raise ValueError("klines_list is not a list")
        if not 200 <= len(klines_list) <= 1000:
            raise ValueError("kline_list length is not between 500 and 1000")
        for i in klines_list:
            if "open" not in i:
                raise ValueError("Some element in kline_list does not contain \" open \" key")
            if "close" not in i:
                raise ValueError("Some element in kline_list does not contain \" close \" key")
            if "high" not in i:
                raise ValueError("Some element in kline_list does not contain \" high \" key")
            if "low" not in i:
                raise ValueError("Some element in kline_list does not contain \" low \" key")
            if "volume" not in i:
                raise ValueError("Some element in kline_list does not contain \" volume \" key")
            if "timestamp" not in i:
                raise ValueError("Some element in kline_list does not contain \" timestamp \" key")
            if "interval" not in i:
                raise ValueError("Some element in kline_list does not contain \" interval \" key")
        return klines_list

    def latest_high(self):
        return self._klines[-1][("high")]

    def latest_low(self):
        return self._klines[-1]["low"]

    def latest_open(self):
        return self._klines[-1]["open"]

    def latest_close(self):
        return self._klines[-1]["close"]

    def latest_volume(self):
        return self._klines[-1]["volume"]

    def latest_timestamp(self):
        return self._klines[-1]["timestamp"]

    def latest_interval(self):
        return self._klines[-1]["interval"]

    def average_volume(self):
        """
        This function returns the average volume of klines_list,
        :return: float
        """
        volumes = []
        for i in self._klines:
            volumes.append(int(i.get("volume")))
        return float(np.mean(volumes))

    def latest_volume_greater_than_average(self):
        """
        Whether the current volume is greater than the mean volume
        :return: True or False
        """
        last_volume = int(self.latest_volume())
        avg_vol = self.average_volume()

        return True if last_volume > avg_vol else False

    def symbol_name(self):
        return self._symbol
