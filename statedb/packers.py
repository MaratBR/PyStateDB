import struct
from functools import partial
from .types import Blob, DTypes, suggest_type, AutoSuggest


def make_packer(fmt):
    return partial(struct.pack, fmt)


def pack_blob(blob):
    if isinstance(blob, bytes):
        blob = Blob(len(blob), blob)
    return struct.pack('=I', blob.size) + blob.data


def pack_bigint(v):
    v = str(v)
    return pack_string(v)


def _pack_string(binary_str: bytes):
    return binary_str + b'\0'


def pack_string(v: str, enc: str='ascii'):
    return _pack_string(v.encode(enc))


pack_utf8_string = partial(pack_string, enc='utf8')


int8_packer = make_packer('b')
uint8_packer = make_packer('B')
int16_packer = make_packer('h')
uint16_packer = make_packer('H')
int32_packer = make_packer('i')
uint32_packer = make_packer('I')
float_packer = make_packer('f')
double_packer = make_packer('d')


def none_packer(_v):
    return b''


packers = {
    DTypes.INT8: int8_packer,
    DTypes.UINT8: uint8_packer,
    DTypes.INT16: int16_packer,
    DTypes.UINT16: uint16_packer,
    DTypes.INT32: int32_packer,
    DTypes.UINT32: uint32_packer,
    DTypes.FLOAT: float_packer,
    DTypes.DOUBLE: double_packer,
    DTypes.NONE: none_packer,
    DTypes.BLOB: pack_blob,
    DTypes.BIGINT: pack_bigint,
    DTypes.STRING: pack_string
}


def get_packer(type_):
    return packers[type_]


def pack(v, type_=AutoSuggest):
    return get_packer(suggest_type(v) if type_ == AutoSuggest else type_)(v)
