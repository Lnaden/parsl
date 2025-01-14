import argparse

import parsl
from parsl.app.app import App
from parsl.data_provider.files import File

from parsl.tests.configs.local_threads import config


@App('bash')
def increment(inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    """.format(inputs=inputs, outputs=outputs)
    return cmd_line


@App('bash')
def slow_increment(dur, inputs=[], outputs=[], stdout=None, stderr=None):
    cmd_line = """
    x=$(cat {inputs[0]})
    echo $(($x+1)) > {outputs[0]}
    sleep {0}
    """.format(dur, inputs=inputs, outputs=outputs)
    return cmd_line


def test_increment(depth=5):
    """Test simple pipeline A->B...->N
    """
    # Create the first file
    open("test0.txt", 'w').write('0\n')

    # Create the first entry in the dictionary holding the futures
    prev = File("test0.txt")
    futs = {}
    for i in range(1, depth):
        print("Launching {0} with {1}".format(i, prev))
        output = File("test{0}.txt".format(i))
        fu = increment(inputs=[prev],  # Depend on the future from previous call
                       # Name the file to be created here
                       outputs=[output],
                       stdout="incr{0}.out".format(i),
                       stderr="incr{0}.err".format(i))
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(
                key), "[TEST] incr failed for key: {0} got: {1}".format(key, data)


def test_increment_slow(depth=5, dur=0.5):
    """Test simple pipeline slow (sleep.5) A->B...->N
    """
    # Create the first file
    open("test0.txt", 'w').write('0\n')

    prev = File("test0.txt")
    # Create the first entry in the dictionary holding the futures
    futs = {}
    print("************** Type: ", type(dur), dur)
    for i in range(1, depth):
        print("Launching {0} with {1}".format(i, prev))
        output = File("test{0}.txt".format(i))
        fu = slow_increment(dur,
                            # Depend on the future from previous call
                            inputs=[prev],
                            # Name the file to be created here
                            outputs=[output],
                            stdout="incr{0}.out".format(i),
                            stderr="incr{0}.err".format(i))
        [prev] = fu.outputs
        futs[i] = prev
        print(prev.filepath)

    for key in futs:
        if key > 0:
            fu = futs[key]
            data = open(fu.result().filepath, 'r').read().strip()
            assert data == str(
                key), "[TEST] incr failed for key: {0} got: {1}".format(key, data)


if __name__ == '__main__':
    parsl.clear()
    dfk = parsl.load(config)

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--width", default="5",
                        help="width of the pipeline")
    parser.add_argument("-d", "--debug", action='store_true',
                        help="Count of apps to launch")
    args = parser.parse_args()

    if args.debug:
        parsl.set_stream_logger()

    # test_increment(depth=int(args.width))
    # test_increment(depth=int(args.width))
    test_increment_slow(depth=int(args.width))
