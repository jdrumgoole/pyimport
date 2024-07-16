'''
Created on 17 Nov 2010

@author: jdrumgoole
'''

import time
import math
from typing import Tuple


def int_remainder(s, factor):
    return (s / factor, s % factor)


def float_remainder(s, factor):
    f = s / factor
    (remainder, divisor) = math.modf(f)
    remainder = remainder * factor
    return divisor, remainder


class PeriodsInSeconds:
    milli = 0.001
    hundredth = 0.01
    tenth = 0.1
    second = 1
    minute = 60 * second
    hour = 60 * minute
    day = 24 * hour
    week = 7 * day
    month = 4 * week
    year = 12 * month


def seconds_to_period(s):
    remainder = s
    (years, remainder) = int_remainder(remainder, PeriodsInSeconds.year)
    (months, remainder) = int_remainder(remainder, PeriodsInSeconds.month)
    (weeks, remainder) = int_remainder(remainder, PeriodsInSeconds.week)
    (days, remainder) = int_remainder(remainder, PeriodsInSeconds.day)
    (hours, remainder) = int_remainder(remainder, PeriodsInSeconds.hour)
    (minutes, remainder) = int_remainder(remainder, PeriodsInSeconds.minute)
    (seconds, remainder) = int_remainder(remainder, PeriodsInSeconds.second)

    (tenths, remainder) = float_remainder(remainder, PeriodsInSeconds.tenth)
    (hundredths, remainder) = float_remainder(remainder, PeriodsInSeconds.hundredth)
    (millis, remainder) = float_remainder(remainder, PeriodsInSeconds.milli)

    return (round(years),
            round(months),
            round(weeks),
            round(days),
            round(hours),
            round(minutes),
            round(seconds),
            round(tenths),
            round(hundredths),
            round(millis))


def mins_secs_hundredths(elapsedSeconds):
    (yrs, mths, wks, days, hours, mins, secs, tenths, hdths, millis) = seconds_to_period(elapsedSeconds)

    return mins, secs, (tenths * 10 + (hdths))


def time_format(hours, mins, hundredths):
    return f"{int(hours)}:{int(mins)}.{int(hundredths)}"


class Timer:
    #
    # A timer object counts elapsed time between  a start event and a stop event.  The timer can be initialised
    # with a time from which counting starts.
    #

    def __init__(self, start_now=False):
        self._start = 0
        self._stop = 0
        self._timer_on = False

        if start_now:
            self.start()

    def start(self):
        self._timer_on = True
        self._start = time.time()
        return self._start

    def reset(self):
        if self._timer_on:
            self._start = time.time()
        else:
            raise ValueError("No timer running")
        return self._start

    def stop(self):
        if self._timer_on:
            self._stop = time.time()
            self._timer_on = False
        return self._stop - self._start

    def is_timing(self):
        return self._timer_on

    def elapsed(self):
        if self._timer_on:
            seconds = time.time() - self._start
        else:
            seconds = self._stop - self._start
        return seconds

    def quantum(self):
        if self._timer_on:
            elapsed = time.time() - self._start

    def __repr__(self):
        mins, secs, hundredths = mins_secs_hundredths(self.elapsed())
        return time_format(mins, secs, hundredths)

    def __str__(self):
        return self.__repr__()


class QuantumTimer(Timer):

    def __init__(self, start_now=False, quantum=None):
        super().__init__(start_now=start_now)
        if quantum:
            self._quantum = quantum
        else:
            self._quantum = 1.0
        self._quantum_written = 0
        self._last_total = 0

    def elapsed_quantum(self, total_written=0) -> Tuple[float, float]:

        elapsed = self.elapsed()
        if elapsed >= self._quantum:
            self._quantum_written = total_written - self._last_total
            self._last_total = total_written
            self.reset()
            docs_per_second = self._quantum_written / elapsed
            #print(f"Quantum: {elapsed} secs, docs per sec: {docs_per_second}, last total : {self._last_total}, total written : {total_written}, quatum written: {self._quantum_written}")
            return elapsed, docs_per_second
        else:
            return 0.0, 0.0


