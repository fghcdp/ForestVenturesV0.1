import math

from scipy.stats import linregress

from burning_house_v1.trading_strategy import TradingStrategy


class LinearRegressionStrategy(TradingStrategy):
    _MINIMUM_KLINE_NUMBER = 200
    _MAXIMUM_KLINE_NUMBER = 1000
    _UPPER_BAND_HEIGHT = 3
    _LOWER_BAND_HEIGHT = 3
    _STRONG_UPTREND_CORRELATION_COEFFICIENT_LIMIT = 0.3
    _STRONG_DOWNTREND_CORRELATION_COEFFICIENT_LIMIT = -0.3
    _REGRESSION_SOURCE = "close"

    def __init__(self, klines_list, interval, symbol):
        TradingStrategy.__init__(self, klines_list, interval, symbol)
        self._dn = float()  # upper deviation: self._reg_m * len(self.klines) + self._reg_c - self._dn
        self._up = float()  # lower deviation: self._reg_m * len(self.klines) + self._reg_c + self._up
        self._reg_m = float()  # factor value
        self._reg_c = float()  # offset value
        self._reg_r = float()  # correlation coefficient value
        self._reg_p = float()  # Pearson value
        self._reg_sd = float()  # standard deviation value
        self.__run()

    def __run(self):
        x_list = range(len(self._klines))
        y_list = []
        for i in self._klines:
            y_list.append(float(i[self._REGRESSION_SOURCE]))

        deviationSum = 0
        slope, intercept, r_value, p_value, std_err = linregress(x_list, y_list)
        for count, i in enumerate(self._klines):
            deviationSum += (float(i[self._REGRESSION_SOURCE]) - (slope * count + intercept)) ** 2
        deviation = math.sqrt(deviationSum / len(self._klines))
        self._dn = deviation * self._UPPER_BAND_HEIGHT
        self._up = deviation * self._LOWER_BAND_HEIGHT
        self._reg_m = slope
        self._reg_c = intercept
        self._reg_r = r_value
        self._reg_p = p_value
        self._reg_sd = std_err

    def __str__(self):
        super().__str__()
        print(
            "%s:INFO: latest upper band price is %s for symbol %s:" % (
                __name__, self.latest_upper_band_price(), self._symbol))
        print(
            "%s:INFO: latest lower band price is %s for symbol %s:" % (
                __name__, self.latest_lower_band_price(), self._symbol))
        print("%s:INFO: is strong uptrend is %s for symbol %s:" % (__name__, self.is_strong_uptrend(), self._symbol))
        print(
            "%s:INFO: is strong downtrend is %s for symbol %s:" % (__name__, self.is_strong_downtrend(), self._symbol))
        print("%s:INFO: Linear Regression Factor is %s for symbol %s:" % (
            __name__, self.regression_factor(), self._symbol))
        print("%s:INFO: Linear Regression Offset is %s for symbol %s:" % (
            __name__, self.regression_offset(), self._symbol))
        print("%s:INFO: Linear Regression Correlation Coefficient is %s for symbol %s:" % (
            __name__, self.regression_correlation_coefficient(), self._symbol))
        print("%s:INFO: Linear Regression Pearson Value is %s for symbol %s: " % (
            __name__, self.regression_pearson_value(), self._symbol))
        print("%s:INFO: Linear Regression Standard Deviation is %s for symbol %s: " % (
            __name__, self.regression_standard_deviation(), self._symbol))
        print("%s:INFO: Linear Regression Upper Band Height is %s for symbol %s: " % (
            __name__, self.regression_upper_band_height(), self._symbol))
        print("%s:INFO: Linear Regression Lower Band Height is %s for symbol %s: " % (
            __name__, self.regression_lower_band_height(), self._symbol))
        print("%s:INFO: Is outside channel is %s for symbol %s:" % (__name__, self.is_outside_channel(), self._symbol))
        print("%s:INFO: Is price higher than upper band is %s for symbol %s:" % (
            __name__, self.is_price_higher_than_upper_band(), self._symbol))
        print("%s:INFO: Is price lower than lower band is %s for symbol %s:" % (
            __name__, self.is_price_lower_than_lower_band(), self._symbol))

    def regression_factor(self):
        return float(self._reg_m)

    def regression_offset(self):
        return float(self._reg_c)

    def regression_correlation_coefficient(self):
        return float(self._reg_r)

    def regression_pearson_value(self):
        return float(self._reg_p)

    def regression_standard_deviation(self):
        return float(self._reg_sd)

    def regression_upper_band_height(self):
        return float(self._up)

    def regression_lower_band_height(self):
        return float(self._dn)

    def latest_upper_band_price(self):
        """
        :return: {float} The Minimum Entry Price of Short Loot, The Upper Band
        """
        return float(self._reg_m * len(self._klines) + self._reg_c + self._up)

    def latest_lower_band_price(self):
        """
        :return: {float} The Maximum Entry Price of Long Loot, The Lower Band
        """
        return float(self._reg_m * len(self._klines) + self._reg_c - self._dn)

    def latest_regression_line_price(self):
        """
        :return: {float} The latest regression line
        """
        return float(self._reg_m * len(self._klines) + self._reg_c)

    def is_strong_uptrend(self):
        """
        Whether the battlefield is in a strong uptrend
        :return: True or False
        """
        if self._reg_r >= self._STRONG_UPTREND_CORRELATION_COEFFICIENT_LIMIT:
            return True
        else:
            return False

    def is_strong_downtrend(self):
        """
        Whether the battlefield is in a strong downtrend
        :return: True or False
        """
        if self._reg_r <= self._STRONG_DOWNTREND_CORRELATION_COEFFICIENT_LIMIT:
            return True
        else:
            return False

    def is_outside_channel(self):
        """
        If close greater than upperband or lower than lowerband
        :return: True or False
        """
        close_price = float(self.latest_close())
        if close_price < self.latest_lower_band_price() or \
                close_price > self.latest_upper_band_price():
            return True
        else:
            return False

    def is_inside_channel(self):
        close_price = float(self.latest_close())
        if close_price > self.latest_lower_band_price() or \
                close_price < self.latest_upper_band_price():
            return True
        else:
            return False

    def is_price_higher_than_upper_band(self):
        close_price = float(self.latest_close())
        if close_price > self.latest_upper_band_price():
            return True
        else:
            return False

    def is_price_lower_than_lower_band(self):
        close_price = float(self.latest_close())
        if close_price < self.latest_lower_band_price():
            return True
        else:
            return False

    def is_price_higher_than_regression_line(self):
        close_price = float(self.latest_close())
        if close_price > self.latest_regression_line_price():
            return True
        else:
            return False

    def is_price_lower_than_regression_line(self):
        close_price = float(self.latest_close())
        if close_price < self.latest_regression_line_price():
            return True
        else:
            return False