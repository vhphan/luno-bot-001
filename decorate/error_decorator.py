import json
import os
import traceback
from datetime import date, datetime
from email.errors import HeaderParseError
from urllib.parse import urlencode
import inspect

from loguru import logger

from decorate.my_email import send_eri_mail

today_str = date.today().strftime('%Y%m%d')
today_time = datetime.now().strftime('%Y%m%d%H%M%S')
THIS_PATH = os.path.dirname(os.path.abspath(__file__))

logger.add(f"{THIS_PATH}/logs/error_decorator_{today_str}.log", rotation="1 MB", backtrace=True, diagnose=True)


def my_exception_handler(e):
    local_args = locals()
    global_args = globals()
    tb = log_traceback(e)
    logger.exception(tb)
    tb = "<br />".join(tb)
    msg = '<br/>'.join(traceback.format_tb(e.__traceback__))
    msg += '<br/>kwargs<br/> ' + json.dumps(local_args.get('kwargs', {}))
    msg += '<br/>args<br/> ' + json.dumps(local_args.get('args', {}))
    msg += f"<br/>traceback, {tb}"
    # msg += '<br/>locals<br/> ' + str(local_args)
    # msg += '<br/>globals<br/> ' + str(global_args)
    try:
        send_eri_mail(message_=msg, subject=f'error {e.__repr__()}')
    except HeaderParseError:
        with open('header_parser.txt', 'a') as f:
            f.write(msg)
        send_eri_mail(message_=e.__repr__(),
                      subject='HeaderParseError',
                      message_type='plain')


def safe_run(func):
    def func_wrapper(*args, **kwargs):

        try:
            return func(*args, **kwargs)

        except Exception as e:
            my_exception_handler(e)
            print(e)
            return f'error?e={str(e)}'

    return func_wrapper


def log_traceback(ex, ex_traceback=None):
    if ex_traceback is None:
        ex_traceback = ex.__traceback__
    return [line.rstrip('\n') for line in
            traceback.format_exception(ex.__class__, ex, ex_traceback)]


def decorate_class(cls, decorate_func=safe_run):
    for name, method in inspect.getmembers(cls, inspect.ismethod):
        setattr(cls, name, decorate_func(method))
    return cls


if __name__ == '__main__':
    @safe_run
    def test_func():
        return 1 / 0


    test_func()
