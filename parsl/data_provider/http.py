import logging
import os
import requests

from parsl import python_app

from parsl.utils import RepresentationMixin
from parsl.data_provider.staging import Staging

logger = logging.getLogger(__name__)

# performs HTTP staging as a separate parsl level task
# TODO: I'm expecting to also implement a Staging method that
# will stage tasks inside a task, for use where there is no
# shared file system.


class HTTPSeparateTaskStaging(Staging, RepresentationMixin):

    def can_stage_in(self, file):
        logger.debug("HTTPSeparateTaskStaging checking file {}".format(file.__repr__()))
        logger.debug("file has scheme {}".format(file.scheme))
        return file.scheme == 'http' or file.scheme == 'https'

    def stage_in(self, dm, executor, file):
        working_dir = dm.dfk.executors[executor].working_dir
        stage_in_app = _http_stage_in_app(dm, executor=executor)
        app_fut = stage_in_app(working_dir, outputs=[file], staging_inhibit_output=True)
        return app_fut._outputs[0]


def _http_stage_in(working_dir, outputs=[], staging_inhibit_output=True):
    file = outputs[0]
    if working_dir:
        os.makedirs(working_dir, exist_ok=True)
        file.local_path = os.path.join(working_dir, file.filename)
    else:
        file.local_path = file.filename
    resp = requests.get(file.url, stream=True)
    with open(file.local_path, 'wb') as f:
        for chunk in resp.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)


def _http_stage_in_app(dm, executor):
    return python_app(executors=[executor], data_flow_kernel=dm.dfk)(_http_stage_in)
