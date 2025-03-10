import argparse
import pickle

import pytest

import parsl
from parsl.app.app import App
from parsl.utils import time_limited_open
from parsl.tests.configs.local_threads_checkpoint_task_exit import config


def local_setup():
    global dfk
    dfk = parsl.load(config)


def local_teardown():
    parsl.clear()


@App('python', cache=True)
def slow_double(x, sleep_dur=1):
    import time
    time.sleep(sleep_dur)
    return x * 2


@pytest.mark.local
@pytest.mark.skip('fails intermittently in pytest')
def test_at_task_exit(n=2):
    """Test checkpointing at task_exit behavior
    """

    d = {}

    print("Launching: ", n)
    for i in range(0, n):
        d[i] = slow_double(i)
    print("Done launching")

    for i in range(0, n):
        d[i].result()

    with time_limited_open("{}/checkpoint/tasks.pkl".format(dfk.rundir), 'rb', seconds=5) as f:
        tasks = []
        try:
            while f:
                tasks.append(pickle.load(f))
        except EOFError:
            pass

        assert len(tasks) == n, "Expected {} checkpoint events, got {}".format(n, len(tasks))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default=10, type=int,
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    x = test_at_task_exit(args.count)
