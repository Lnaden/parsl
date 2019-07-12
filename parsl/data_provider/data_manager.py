import logging

from parsl.data_provider.file_noop import NoOpFileStaging
from parsl.data_provider.ftp import FTPSeparateTaskStaging
# from parsl.data_provider.http import HTTPSeparateTaskStaging
from parsl.data_provider.http import HTTPInTaskStaging

logger = logging.getLogger(__name__)

# these will be shared between all executors that do not explicitly
# override, so should not contain executor-specific state

# UNDO THIS: in this patch, this changes to http in-task staging for ease of my
# testing, but it should default to HTTPSeparateTaskStaging to keep consistency
# with earlier versions.
defaultStaging = [NoOpFileStaging(), FTPSeparateTaskStaging(), HTTPInTaskStaging()]


class DataManager(object):
    """The DataManager is responsible for transferring input and output data.

    """

    def __init__(self, dfk):
        """Initialize the DataManager.

        Args:
           - dfk (DataFlowKernel): The DataFlowKernel that this DataManager is managing data for.

        Kwargs:
           - executors (list of Executors): Executors for which data transfer will be managed.
        """

        self.dfk = dfk
        self.globus = None

    def replace_task(self, file, func, executor):
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(scheme))
            if scheme.can_stage_in(file):
                return scheme.replace_task(self, executor, file, func)

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def replace_task_stage_out(self, file, func, executor):
        """This will give staging providers the chance to wrap (or replace entirely!) the task function."""
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(scheme))
            if scheme.can_stage_out(file):
                return scheme.replace_task_stage_out(self, executor, file, func)

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_in(self, file, executor, parent_fut):
        """Transport the file from the input source to the executor.

        This function returns a DataFuture.

        Args:
            - self
            - file (File) : file to stage in
            - executor (str) : an executor the file is going to be staged in to.
            - parent_fut: (optional DataFuture) : If specified, stage in tasks will depend on this
                future completing before executing, and the File contained in that future will be
                used as input.
        """
        if parent_fut is None:
            raise ValueError("BENC: non-None parent_fut")

        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access

        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_in checking Staging provider {}".format(scheme))
            if scheme.can_stage_in(file):
                return scheme.stage_in(self, executor, file)

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage file {}".format(executor, repr(file)))

    def stage_out(self, file, executor, app_fu):
        """Transport the file from the local filesystem to the remote Globus endpoint.

        This function returns ? (previously documentated as a DataFuture - but not used)

        Args:
            - self
            - file (File) - file to stage out
            - executor (str) - Which executor the file is going to be staged out from.
        """
        executor_obj = self.dfk.executors[executor]
        if hasattr(executor_obj, "storage_access") and executor_obj.storage_access is not None:
            storage_access = executor_obj.storage_access
        else:
            storage_access = defaultStaging

        for scheme in storage_access:
            logger.debug("stage_out checking Staging provider {}".format(scheme))
            if scheme.can_stage_out(file):
                return scheme.stage_out(self, executor, file, app_fu)

        logger.debug("reached end of staging scheme list")
        # if we reach here, we haven't found a suitable staging mechanism
        raise ValueError("Executor {} cannot stage out file {}".format(executor, repr(file)))
