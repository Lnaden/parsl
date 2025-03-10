import argparse
import os

import parsl
from parsl.app.app import App
from parsl.tests.configs.local_threads import fresh_config

local_config = fresh_config()
local_config.retries = 2


@App('python')
def sleep_then_fail(inputs=[], sleep_dur=0.1):
    import time
    import math
    time.sleep(sleep_dur)
    math.ceil("Trigger TypeError")
    return 0


@App('bash')
def succeed_on_retry(filename, success_on=2, stdout="succeed.out"):
    """If the input file does not exist it creates it.
    Then, if the file contains success_on lines it exits with 0
    """

    return """if [[ ! -e {0} ]]; then touch {0}; fi;
    tries=`wc -l {0} | cut -f1 -d' '`
    echo $tries >> {0}

    if [[ "$tries" -eq "{success_on}" ]]
    then
        echo "Match. Success"
    else
        echo "Tries != success_on , exiting with error"
        exit 5
    fi
    """


@App('python')
def sleep(sleep_dur=0.1):
    import time
    time.sleep(sleep_dur)
    return 0


def test_fail_nowait(numtasks=10):
    """Test retries on tasks with no dependencies.
    """
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(sleep_dur=0.1)
        fus.extend([fu])

    try:
        [x.result() for x in fus]
    except Exception as e:
        assert isinstance(
            e, TypeError), "Expected a TypeError, got {}".format(e)

    print("Done")


def test_fail_delayed(numtasks=10):
    """Test retries on tasks with dependencies.

    This is testing retry behavior when AppFutures are created
    with no parent.
    """

    x = sleep()
    fus = []
    for i in range(0, numtasks):
        fu = sleep_then_fail(inputs=[x], sleep_dur=0.5)
        fus.extend([fu])

    try:
        [x.result() for x in fus]
    except Exception as e:
        assert isinstance(
            e, TypeError), "Expected a TypeError, got {}".format(e)

    print("Done")


def test_retry():
    """Test retries via app that succeeds on the Nth retry.
    """

    fname = "retry.out"
    try:
        os.remove(fname)
    except OSError:
        pass
    fu = succeed_on_retry(fname)

    fu.result()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--count", default="10",
                        help="Count of apps to launch")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    test_fail_nowait(numtasks=int(args.count))
    # test_fail_delayed(numtasks=int(args.count))
    # test_retry()
