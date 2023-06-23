import contextlib
import os


@contextlib.contextmanager
def temp_env_vars(**kwargs):
    """
    Temporarily set the process environment variables.
    >>> with temp_env_vars(PLUGINS_DIR=u'test/plugins'):
    ...   "PLUGINS_DIR" in os.environ
    True
    >>> "PLUGINS_DIR" in os.environ
    """
    old_environ = dict(os.environ)
    kwargs = {k: str(v) for k, v in kwargs.items()}
    os.environ.update(**kwargs)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)
