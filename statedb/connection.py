import logging
import sys
from collections import namedtuple
from threading import Thread
from typing import Tuple, Callable, Optional


from statedb.protocol import *
from statedb.typing import DataType, Value


class Connection:
    MessageHandler = Callable[[object, 'Connection'], None]

    def __init__(self, ip, port):
        self._socket = socket.socket()
        self.port = port
        self.ip = ip
        self._isopen = False
        self._handlers = {}
        self._recv_thread = None
        self._is_listening = False
        self.storage = Storage(self)

        self._logger = logging.getLogger('StateDB')
        self._logger.setLevel(logging.DEBUG)
        hndlr = logging.StreamHandler(sys.stdout)
        hndlr.setFormatter(logging.Formatter('%(asctime)s %(filename)s %(levelname)s : %(message)s'))
        hndlr.setLevel(logging.DEBUG)
        self._logger.addHandler(hndlr)

        self._logger.debug('Connection created')
        self.register(Response.E.ERROR, self._on_error)

    def register(self, id_: Response.E, handler: MessageHandler = None):
        if handler is None:
            def decorator(f):
                self.register(id_, f)
                return f

            return decorator
        if id_ not in self._handlers:
            self._handlers[id_] = [handler]
        else:
            self._handlers[id_].append(handler)

    def connect(self):
        self._socket.connect((self.ip, self.port))
        self.send(HELLO1)
        self._isopen = True

    def _prepare_for_listening(self):
        self._is_listening = True

    @property
    def can_listen(self):
        return self._is_listening and self._isopen

    def listen_thread(self):
        if self._recv_thread is None:
            self._recv_thread = Thread(target=self.listen_loop, daemon=True)
            self._recv_thread.start()

    def listen_loop(self):
        if not self._is_listening:
            self._is_listening = True
        else:
            return
        self._logger.debug('Loop entered')
        while self.can_listen:
            self.listen_iteration()
        self._logger.debug('Loop exited')

    def listen_iteration(self):
        preamble = self._recv_preamble()
        body = preamble.load_body(self._socket) if preamble.has_body_loader() else preamble.body
        self._handle_message(preamble.id, body)

    def send(self, obj):
        self._socket.send(bytes(obj))

    def _handle_message(self, id_: Response.E, body):
        if id_ in self._handlers:
            for h in self._handlers[id_]:
                h(body, self)

    def _on_error(self, err: ErrorBody):
        self._logger.error(f'Server error: {err.message}')

    def _recv_preamble(self):
        preamble = ResponsePreamble(self._socket)
        self._logger.debug(f'Received preamble: {preamble.id}')
        return preamble


class Storage(dict):
    VDTPair = namedtuple('VDTPair', ['py_value', 'dtype'])

    def __init__(self, conn: 'Connection'):
        super(Storage, self).__init__()
        self._types = {}
        self._awaiting_keys = set()
        self._conn = conn
        self._conn.register(Response.E.VALUE, self._on_new_value)
        self._conn.register(Response.E.DELETED, self._on_deleted)

    def update_all(self):
        self._conn.send(GetAll())

    def request_value(self, key):
        self._conn.send(Get(key))

    def _set(self, key, val, forced=False):
        if not forced and key in self and val == self[key]:
            return
        dtype, val = self._as_value(val)
        self._conn.send(Set(key, val, dtype))
        self._awaiting_keys.add(key)

    def _on_new_value(self, body: NewValueBody, _: Connection):
        dict.__setitem__(self, body.key, body.value)
        self._types[body.key] = body.dtype
        if body.key in self._awaiting_keys:
            self._awaiting_keys.remove(body.key)

    def _on_deleted(self, body: KeyDeletedBody, _: Connection):
        if body.key in self:
            super(Storage, self).__delitem__(body.key)
            del self._types[body.key]

    def __setitem__(self, key, value):
        self._set(key, value, True)

    def __delitem__(self, key):
        self._conn.send(Delete(key))

    def get_pair(self, k: str) -> VDTPair:
        return self.VDTPair(self[k], self._types[k])

    def get_type(self, k: str):
        return self._types[k]

    def get(self, k: str, default: Value = None) -> Optional[Value]:
        try:
            return self[k]
        except KeyError:
            return default

    @staticmethod
    def _as_value(val) -> Tuple[DataType, object]:
        if isinstance(val, tuple):
            return val[0], val[1]
        return AutoSuggest, val
