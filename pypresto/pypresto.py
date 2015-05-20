from future import standard_library
standard_library.install_aliases()
from builtins import object
import httplib2
import random
import json
import logging
import queue

from time import sleep
from concurrent.futures import ThreadPoolExecutor
from threading import Event

__all__ = ['PyPrestoError', 'Client']
__version__ = "0.0.1"


logger = logging.getLogger(__name__)


class PyPrestoError(Exception):
    pass


class Query(object):
    def __init__(self, session, q, args=None):
        self.result = queue.Queue()
        self.columns = None
        self.session = session
        self.query_string = self.__process_statement(q, args)
        self.done = Event()
        self.communication = []
        self.state = 'UNKNOWN'
        self.info_url = None
        self.next_url = None
        self.headers = {"X-Presto-Catalog":  self.session.catalog,
                        "X-Presto-Source":   "pypresto %s" % __version__,
                        "X-Presto-Schema":   self.session.schema,
                        "X-Presto-User":     self.session.user}

        self.current_step = 0

    def execute_wait(self):
        self.execute(wait=True)
        return self

    def execute(self, wait=False):
        self.__execute()
        if wait is True:
            self.wait()
        return self

    def wait(self):
        while not self.done.is_set():
            self.__check_status()
            if self.current_step < self.session.wait_max_step:
                self.current_step += self.session.wait_step
            sleep(self.current_step)

    def __execute(self):
        url = "http://%s:%d/v1/statement" % (random.choice(self.session.hostnames), self.session.port)
        rsp = self.__request(url, "POST", body=self.query_string)

        if 'stats' not in rsp or 'state' not in rsp['stats']:
            raise PyPrestoError('No state in response: %s' % (url))

        self.info_url = rsp['infoUri']
        self.next_url = rsp['nextUri']
        self.state = rsp['stats']['state']

    def __check_status(self):
        if self.next_url is None:
            self.done.set()
            return

        rsp = self.__request(self.next_url, "GET")
        self.state = rsp['stats']['state']

        if 'infoUri' in rsp:
            self.info_url = rsp['infoUri']
        if 'nextUri' in rsp:
            self.next_url = rsp['nextUri']
        if 'columns' in rsp:
            self.columns = rsp['columns']
        if 'data' in rsp:
            for row in rsp['data']:
                if self.session.result_mode == 'dict':
                    try:
                        self.result.put_nowait(dict([(self.columns[pos]['name'], value) for pos, value in enumerate(row)]))
                    except IndexError:
                        raise PyPrestoError('Mismatch between columns and row: %r' % (row))
                else:
                    self.result.put_nowait(row)

        if self.state == 'FAILED':
            self.done.set()
            raise PyPrestoError('Query failed: %s, %r' % (self.query_string, rsp['error']))
        elif self.state == 'FINISHED':
            logger.info('Query finished: %s', self.query_string)
            self.done.set()

    def __process_statement(self, q, args):
        if isinstance(args, tuple):
            args = list(args)
        elif args is None:
            args = list()

        return q % tuple([self.__escape(arg) for arg in args])

    def __escape(self, arg):
        return arg

    def __request(self, url, method="GET", body=None):
        h = httplib2.Http()
        logger.debug('Calling: %s', url)
        rsp, content = h.request(url, method, body=body, headers=self.headers)
        if rsp.status != 200:
            raise PyPrestoError('Error calling: %s, %s' % (url))

        return json.loads(content)

    def iter_results(self):
        while True:
            if self.done.is_set() and self.result.empty():
                return

            # do a sleepover
            if not self.done.is_set():
                self.__check_status()
                if self.current_step < self.session.wait_max_step:
                    self.current_step += self.session.wait_step
                sleep(self.current_step)

            if not self.result.empty():
                try:
                    while True:
                        yield self.result.get_nowait()
                except queue.Empty:
                    pass


class Session(object):
    def __init__(self, hostnames=None, port=8080, max_workers=6, catalog="hive", schema="default", user='nobody', result_mode='dict'):
        self.catalog = catalog
        self.schema = schema
        self.hostnames = hostnames
        self.port = port
        self.user = user
        self.result_mode = result_mode

        self.executor = ThreadPoolExecutor(max_workers=max_workers)

        self.default_timeout = None
        self.wait_step = 0.2
        self.wait_max_step = 10

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.executor.shutdown()

    def query_async(self, q, args=None):
        q = Query(self, q, args)
        return self.executor.submit(q.execute_wait)

    def query(self, q, args=None):
        q = Query(self, q, args)
        q.execute()
        return q


class Client(object):
    def __init__(self, hostnames, user='nobody', port=8080):
        self.hostnames = hostnames
        self.port = port
        self.user = user

    def connect(self, *argv, **kwargs):
        kwargs['hostnames'] = self.hostnames
        kwargs['port'] = self.port
        kwargs['user'] = self.user
        return Session(*argv, **kwargs)
