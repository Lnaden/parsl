import logging
import os

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)

# this provider should be parameterised with enough information
# to connect back to the rsync server, and understand how to
# form a path - similar to the globus provider

# for now, it will only accept absolute paths, because those don't
# involve me normalising the path to put in url

# if not using ssh but the rsync protocol, the initializer for this
# staging mechanism could set up the rsync server on the submit
# side, because __init__ is a hook where it can do that kind of stuff
# although there isn't a closedown hook at the moment


class FileRsyncStaging(Staging, RepresentationMixin):

    def __init__(self, server_prefix: str = "SPECIFY_A_SERVER_PREFIX_WORKAROUND_ISSUE_1124"):
        """
        server_prefix: server/path to prefix onto the filename to access the rsync server
        """
        self.server_prefix = server_prefix

    def can_stage_in(self, file):
        logger.debug("FileRsyncStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def can_stage_out(self, file):
        logger.debug("FileRsyncStaging checking file {} for stageout".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'file'

    def replace_task(self, dm, executor, file, f):
        working_dir = dm.dfk.executors[executor].working_dir
        return in_task_transfer_wrapper(f, file, working_dir, self.server_prefix)


def in_task_transfer_wrapper(func, file, working_dir, server_prefix):
    def wrapper(*args, **kwargs):
        if working_dir:
            print("BENC: workdir_dir set")
            os.makedirs(working_dir, exist_ok=True)
            file.local_path = os.path.join(working_dir, file.filename)
        else:
            print("BENC: workdir_dir not set")
            file.local_path = file.filename
        print("BENC: file.local_path = {}".format(file.local_path))
        rsync_path = server_prefix + "/" + file.filename

        cmd = "rsync --verbose {} {}".format(rsync_path, file.local_path)
        ret = os.system(cmd)
        if ret != 0:
            raise ValueError("rsync returned {}".format(ret))

        # resp = requests.get(file.url, stream=True)
        # with open(file.local_path, 'wb') as f:
        #    for chunk in resp.iter_content(chunk_size=1024):
        #        if chunk:
        #            f.write(chunk)

        result = func(*args, **kwargs)
        return result
    return wrapper
