''' Testing simple python apps
'''
import parsl
from parsl import *

import os
import time
import shutil
import argparse
import random

workers = ThreadPoolExecutor(max_workers=4)
dfk = DataFlowKernel(executors=[workers])


@App('python', dfk)
def increment(x):
    return x + 1


@App('python', dfk)
def slow_increment(x, dur):
    import time
    time.sleep(dur)
    return x + 1


def test_increment(depth=5):
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = increment(futs[i - 1])

    x = sum([futs[i].result() for i in futs if type(futs[i]) != int])
    assert x == sum(range(1, depth)), "[TEST] increment [FAILED]"


def test_slow_increment(depth=5):
    futs = {0: 0}
    for i in range(1, depth):
        futs[i] = slow_increment(futs[i - 1], 0.01)

    x = sum([futs[i].result() for i in futs if type(futs[i]) != int])

    assert x == sum(range(1, depth)), "[TEST] slow_increment [FAILED]"


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5", help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true', help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    tests = [test_increment, test_slow_increment]
    for width in [10, 100, 1000]:
        for test in tests:
            try:
                test(depth=int(width))

            except AssertionError as e:
                print("[TEST]  %s width:%s [FAILED]" % (test.__name__, width))
                print(e)

            else:
                print("[TEST]  %s width:%s type [SUCCESS]" % (test.__name__, width))
