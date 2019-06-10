import io
from socket import socket
import struct
import typing
from functools import partial

from statedb.typing import *
from .types import Blob, DTypes, suggest_type, AutoSuggest


def make_unpacker(fmt) -> Unpacker:
    inner = partial(struct.unpack, fmt)

    def unpacker(v):
        if isinstance(v, socket):
            v = v.recv(struct.Struct(fmt).size)
        elif isinstance(v, io.BytesIO):
            v = v.read(struct.Struct(fmt).size)
        return inner(v)[0]

    return unpacker


def unpack_blob(data) -> Blob:
    size_ = unpack(data, DTypes.UINT32)
    if isinstance(data, io.BytesIO):
        data = data.read(size_)
    elif isinstance(data, socket):
        data = data.recv(size_)
    return Blob(size=size_, data=data[4:])


def unpack_bigint(data) -> int:
    value = unpack_string(data)
    return int(value)


def _read_until_zero(reader) -> bytes:
    key = b''
    while True:
        b = reader()
        if b == b'\0':
            break
        key += b
    return key + b'\0'


def unpack_string(data, enc: str = 'ascii') -> str:
    if isinstance(data, io.BytesIO):
        data = _read_until_zero(partial(io.BytesIO.read, data, 1))
    elif isinstance(data, socket):
        data = _read_until_zero(partial(socket.recv, data, 1))

    return data[:-1].decode(enc)


unpack_utf8_string = partial(unpack_string, enc='utf8')

int8_unpacker = make_unpacker('b')
uint8_unpacker = make_unpacker('B')
int16_unpacker = make_unpacker('h')
uint16_unpacker = make_unpacker('H')
int32_unpacker = make_unpacker('i')
uint32_unpacker = make_unpacker('I')
float_unpacker = make_unpacker('f')
double_unpacker = make_unpacker('d')
string_unpacker = unpack_string


def none_unpacker(_d):
    return None


unpackers: typing.Dict[DTypes, Unpacker] = {
    DTypes.INT8: int8_unpacker,
    DTypes.UINT8: uint8_unpacker,
    DTypes.INT16: int16_unpacker,
    DTypes.UINT16: uint16_unpacker,
    DTypes.INT32: int32_unpacker,
    DTypes.UINT32: uint32_unpacker,
    DTypes.FLOAT: float_unpacker,
    DTypes.DOUBLE: double_unpacker,
    DTypes.NONE: none_unpacker,
    DTypes.BLOB: unpack_blob,
    DTypes.BIGINT: unpack_bigint,
    DTypes.STRING: unpack_string
}


def get_unpacker(type_):
    return unpackers[type_]


def unpack(v, type_: typing.Union[DTypes]) -> Value:
    return get_unpacker(type_)(v)
