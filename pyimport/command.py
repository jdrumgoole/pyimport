"""

Author: joe@joedrumgoole.com

5-May-2018

"""
import logging
from datetime import datetime, timedelta


def seconds_to_duration(seconds):
    result=""
    delta = timedelta(seconds=seconds)
    d = datetime(1, 1, 1) + delta
    if d.day - 1 > 0:
        result =f"{d.day -1} day(s)"
    result = result + "%02d:%02d:%02d.%02d" % (d.hour, d.minute, d.second, d.microsecond)
    return result


def input_prompt(prompt: str, response: list[str], default: str = None) -> str | None:
    if default is not None:
        prompt = f"{prompt} [{default}]"
    else:
        prompt = f"{prompt} [{response[0]}]"
    user_input = input(prompt)
    if user_input.lower().strip() in response:
        return user_input
    elif user_input == "":
        return default
    else:
        return None


class Command:

    def __init__(self, audit=None, args=None):
        pass

    def run(self, args):
        pass




