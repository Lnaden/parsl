template_string = """#!/bin/bash
cd ~
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y python3 python3-pip libffi-dev g++ libssl-dev git
pip3 install numpy scipy
git clone https://github.com/Parsl/parsl/
pushd parsl
git checkout 86120f5364548d37d8fd9a90a6628b0c6986b772
pip3 install .
popd

$worker_init

$user_script

# Shutdown the instance as soon as the worker scripts exits
# or times out to avoid EC2 costs.
if ! $linger
then
    halt
fi
"""
