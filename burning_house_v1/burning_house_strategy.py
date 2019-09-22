from copy import deepcopy

from burning_house_v1.battlefield_situation import BattleFieldSituation
from burning_house_v1.linear_regression_strategy import LinearRegressionStrategy


class BurningHouseStrategy(LinearRegressionStrategy):

    # noinspection SpellCheckingInspection,PyPep8Naming
    def __init__(self, klines_list, interval, symbol):
        """"
        NOTE: This is a strategy that is well suited for day trading,
        for example, in crypto market, the sensor senses the chaos moment
        in the market and returns some useful info for trading bot to trade
        the ticker
        NOTE: This strategy DOES NOT provide stop loss info, you need to
        calculate the stop loss price according to your leverage.
        CAPABILITY: From 0.02 BTC with 10x to 20x Leverage to up to 100K USD
        :param {list} klines_list:
        [{
            "high": "15690",
            "low": "15670",
            "close": "15680",
            "timestamp": 1569041656,
            "interval": "1m"
            "volume": "3",
        },{...}]
        :param {str} interval
        :param {str} symbol
        """
        LinearRegressionStrategy.__init__(self, deepcopy(klines_list), interval, symbol)

    # noinspection SpellCheckingInspection
    def __str__(self):
        LinearRegressionStrategy.__str__(self)
        print("%s:INFO: Battlefield Situation is %s for symbol %s:" % (
            __name__, self.battlefield_situtation(), self._symbol))
        print("%s:INFO: Latest Exit Price is %s for symbol %s:" % (__name__, self.latest_exit_price(), self._symbol))
        return ""

    # noinspection SpellCheckingInspection
    def battlefield_situtation(self) -> str:
        """
        Whether the sensor saw a fire in battlefield
        """
        if self.is_strong_uptrend() and \
                self.is_outside_channel() and \
                self.is_price_higher_than_upper_band():
            return BattleFieldSituation.SHORTENTRY
        elif self.is_strong_downtrend() and \
                self.is_outside_channel() and \
                self.is_price_lower_than_lower_band():
            return BattleFieldSituation.LONGENTRY
        elif self.is_inside_channel() and \
                self.is_price_higher_than_regression_line():
            return BattleFieldSituation.LONGEXIT
        elif self.is_inside_channel() and \
                self.is_price_lower_than_regression_line():
            return BattleFieldSituation.SHORTEXIT

    def latest_exit_price(self):
        return self.regression_factor() * len(self._klines) + self.regression_offset()
