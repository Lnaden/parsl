from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.data_provider.file_rsync import FileRsyncStaging

config = Config(
    executors=[
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd1/", label="t1"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd2/", label="t2"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd3/", label="t3"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd4/", label="t4"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd5/", label="t5"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd6/", label="t6"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd7/", label="t7"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd8/", label="t8"),
      ThreadPoolExecutor(storage_access=[FileRsyncStaging("172.17.0.1:/home/benc/parsl/src/parsl/")], working_dir="/home/benc/parsl/tmp/wd9/", label="t9")
    ])
