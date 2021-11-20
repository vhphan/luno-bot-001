import time
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
