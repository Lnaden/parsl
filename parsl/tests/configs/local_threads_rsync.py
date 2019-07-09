from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.data_provider.file_rsync import FileRsyncStaging

config = Config(
    executors=[ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd1/")])
