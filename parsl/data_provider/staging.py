
# this defines the interface that staging methods should have

# the data manager will present a file for staging to each
# Staging mechanism in turn, using can_stage_in, until it
# finds a Staging provider that can perform the staing.
# can_stage_in might be called multiple times during job
# submission, and it must return the same result each time
# for a given file.

# once the staging provider has been found, the stage_in hook
# will be called, allowing the Staging provider to substitute
# a file for a DataFuture that (when complete) means the file
# is in place.

# it is expected that other hooks will be put in place, such as
# substituting the entire app task for a new one (such as the
# original one wrapped with a staging wrapper)

# the Staging class should be subclassed by mechanism specific
# classes, and appropriate methods overridden. The default
# implementations of each method do nothing / make no changes
# and should be safe to leave in place if there is no
# overriding behaviour.


class Staging:

    # given a File object, decide if this staging provider can
    # stage the file. Commonly this will be based on the URI
    # scheme, but does not have to be. If this returns True,
    # then other methods of this Staging object will be called.
    def can_stage_in(self, file):
        return False

    # for a given file, either return a DataFuture to substitute
    # for this file, or return None to perform no substitution
    def stage_in(self, dm, executor, file):
        return None

    # for a file to be staged in, give the provider the chance to
    # replace (eg wrap with staging wrapper) the app function.
    # if this returns None, no substitution happens
    def replace_task(self, dm, executor, file, func):
        return None
