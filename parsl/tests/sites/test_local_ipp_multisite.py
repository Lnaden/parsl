import pytest

from parsl.app.app import App, bash_app
from parsl.tests.configs.local_ipp_multisite import config


local_config = config


@App("python", executors=['local_ipp_2'])
def python_app_2():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@App("python", executors=['local_ipp_1'])
def python_app_1():
    import os
    import threading
    import time
    time.sleep(1)
    return "Hello from PID[{}] TID[{}]".format(os.getpid(), threading.current_thread())


@bash_app
def bash(stdout=None, stderr=None):
    return 'echo "Hello from $(uname -a)" ; sleep 2'


@pytest.mark.local
@pytest.mark.skip('broken in pytest')
def test_python(N=2):
    """Testing basic python functionality."""

    r1 = {}
    r2 = {}
    for i in range(0, N):
        r1[i] = python_app_1()
        r2[i] = python_app_2()
    print("Waiting ....")

    for x in r1:
        print("python_app_1: ", r1[x].result())
    for x in r2:
        print("python_app_2: ", r2[x].result())

    return


@pytest.mark.local
def test_bash():
    """Testing basic bash functionality."""

    import os
    fname = os.path.basename(__file__)

    x = bash(stdout="{0}.out".format(fname))
    print("Waiting ....")
    print(x.result())
