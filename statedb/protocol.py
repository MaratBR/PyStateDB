import io
import socket
from enum import Enum

from statedb.types import AutoSuggest, suggest_type, DTypes
from .packers import uint8_packer, pack, pack_string
from .unpackers import uint8_unpacker, unpack_string, unpack
import struct


def pack_dtype(dtype: DTypes):
    return uint8_packer(dtype.value)


def unpack_dtype(data: bytes):
    return DTypes(uint8_unpacker(data))


pack_key = pack_string
unpack_key = unpack_string


def _get_int(v):
    if isinstance(v, Enum):
        return v.value
    return int(v)


class Preamble:
    def __init__(self, id_, size, flags=0):
        self.id = id_
        self.size = size
        self.flags = flags

    def __bytes__(self):
        return struct.pack('=HBI', self.id, self.flags, self.size)


class Message:
    class E(Enum):
        GET = 1
        DELETE = 2
        PING = 3
        SET = 4
        GET_ALL = 5

    def __init_subclass__(cls, **kwargs):
        if 'message_id' not in kwargs:
            raise LookupError('message_id key must be specified when inheriting from Message class.\n'
                              '\tLike so:\n'
                              f'\t\tclass {cls.__name__}(Message, message_id=<message_id>):\n'
                              f'\t\t\t...')
        message_id = kwargs.pop('message_id')

        def default__bytes__(self):
            return b''
        old__bytes__ = getattr(cls, '__bytes__', default__bytes__)

        def new__bytes__(self):
            inner = old__bytes__(self)
            return bytes(Preamble(_get_int(message_id), len(inner))) + inner

        cls.__bytes__ = new__bytes__
        return super(Message, cls).__init_subclass__(**kwargs)


class Get(Message, message_id=Message.E.GET):
    def __init__(self, key):
        self.key = key

    def __bytes__(self):
        return pack_key(self.key)


class Set(Message, message_id=Message.E.SET):
    def __init__(self, key, value, dtype=AutoSuggest):
        self.key = key
        self.value = value
        self._dtype = dtype

    @property
    def dtype(self):
        return suggest_type(self.value) if self._dtype == AutoSuggest else self._dtype

    @dtype.setter
    def dtype(self, val):
        self._dtype = val

    def auto_suggest_type(self):
        self._dtype = AutoSuggest

    def __bytes__(self):
        return pack_key(self.key) + pack_dtype(self.dtype) + pack(self.value, self.dtype)


class Delete(Message, message_id=Message.E.DELETE):
    def __init__(self, key):
        self.key = key

    def __bytes__(self):
        return pack_key(self.key)


class Ping(Message, message_id=Message.E.PING):
    pass


class GetAll(Message, message_id=Message.E.GET_ALL):
    pass


class Hello:
    def __init__(self, protocol):
        self.protocol = 1

    def __bytes__(self):
        return b'HeLlO' + pack(self.protocol, DTypes.UINT8)


HELLO1 = Hello(1)


def response_code(v):
    return (0xFF << 8) | v


class Response:
    class E(Enum):
        VALUE = response_code(1)
        DELETED = response_code(2)
        FORCE_LOGOUT = response_code(3)
        PONG = response_code(4)
        ERROR = response_code(0xEE)


class ResponsePreamble:
    FORMAT = '=HI'
    _struct = struct.Struct(FORMAT)

    def __init__(self, src):
        if isinstance(src, socket.socket):
            src = src.recv(self._struct.size)
        self.id, self.size = self._struct.unpack(src)
        self.id = Response.E(self.id)
        self.body = None
        self._body_loaded = False

    def load_body(self, s):
        if not self._body_loaded:
            data = s.recv(self.size)
            print(data)
            self.body = body_handlers.get(self.id, fallback_body_handler)(data)
        return self.body

    def has_body_loader(self):
        return self.id in body_handlers


class NewValueBody:
    def __init__(self, data: bytes):
        io_ = io.BytesIO(data)
        self.key = unpack_key(io_)
        self.dtype = unpack_dtype(io_)
        self.value = unpack(io_, self.dtype)
        a = 3


class KeyDeletedBody:
    def __init__(self, data: bytes):
        self.key = unpack(data, DTypes.STRING)


class ErrorBody:
    def __init__(self, data: bytes):
        self.message = unpack(data, DTypes.STRING)


body_handlers = {
    Response.E.VALUE: NewValueBody,
    Response.E.DELETED: KeyDeletedBody,
    Response.E.ERROR: ErrorBody
}


def fallback_body_handler(_s):
    pass
