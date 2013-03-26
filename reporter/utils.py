import os
import tempfile


class TemporaryDirectory(object):
    """Create and return a temporary directory.  This has the same
    behavior as mkdtemp but can be used as a context manager.  For
    example:

        with TemporaryDirectory() as tmpdir:
            ...

    Upon exiting the context, the directory and everthing contained
    in it are removed.

    Backported from Python 3.2+
    """

    def __init__(self, suffix="", prefix='tmp', dir=None):
        self.name = tempfile.mkdtemp(suffix, prefix, dir)
        self._closed = False

    def __enter__(self):
        return self.name

    def cleanup(self):
        if not self._closed:
            self._rmtree(self.name)
            self._closed = True

    def __exit__(self, exc, value, tb):
        self.cleanup()

    def _rmtree(self, path):
        # Essentially a stripped down version of shutil.rmtree.  We can't
        # use globals because they may be None'ed out at shutdown.
        for name in os.listdir(path):
            fullname = os.path.join(path, name)
            try:
                isdir = os.path.isdir(fullname)
            except os.error:
                isdir = False
            if isdir:
                self._rmtree(fullname)
            else:
                try:
                    os.remove(fullname)
                except os.error:
                    pass
        try:
            os.rmdir(path)
        except os.error:
            pass
