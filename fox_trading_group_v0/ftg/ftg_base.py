import time


# noinspection PyUnresolvedReferences
class FTGBase(object):
    def start_trading(self):
        while True:
            if self.bybit.warm_up_finished():
                self._assert_data_stream_alive()
                start_time = time.time()
                self._analyze()
                seconds_elasped = time.time() - start_time
                self._report(seconds_elasped)
                self._trade()
                self._trade_round += 1
                self.__sleep()
            else:
                self.__sleep()

    def __sleep(self):
        time.sleep(0.25)
