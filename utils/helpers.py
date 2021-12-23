import time
import types
from datetime import datetime


def milli_to_dt(ms):
    return datetime.fromtimestamp(ms / 1000.0)


def dt_to_milli(dt: str) -> int:
    dt_obj = datetime.strptime(dt, '%Y-%m-%d')
    return int(dt_obj.timestamp() * 1000)


def current_to_milli_time():
    return int(time.time() * 1000)


def is_method(obj, name):
    return hasattr(obj, name) and isinstance(getattr(obj, name), types.MethodType)


def get_methods_in_obj(c):
    results = []
    for attr in dir(c):
        if attr.startswith('__') and attr.endswith('__'):
            continue
        if is_method(c, attr):
            results.append(attr)
    return results


class TooManyTriesException(BaseException):
    def __repr__(self):
        return 'Too many retries with exception'


# adapted from https://gist.github.com/alairock/a0235eae85c62f0f0f7b81bec8aa378a
def async_retry(exceptions, retries, logger=None):
    def func_wrapper(f):
        async def wrapper(*args, **kwargs):
            for retry in range(retries):
                if logger is not None:
                    logger.info(f'retry #: {retry + 1}')
                else:
                    print('times:', retry + 1)
                # noinspection PyBroadException
                try:
                    return await f(*args, **kwargs)
                except exceptions as exc:
                    logger.error(exc)
            logger.error(TooManyTriesException)
            raise TooManyTriesException()

        return wrapper

    return func_wrapper
