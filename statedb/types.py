from enum import Enum


class Blob:
    def __init__(self, size, data):
        self.size = size
        self.data = data

    def __repr__(self):
        data_repr = str(self.data)[2:-1]
        if len(data_repr) > 20:
            data_repr = data_repr[:17] + '...'
        return f'<Blob size={self.size} data={data_repr}>'


_USE_DOUBLE_PRECISION = True


def use_double_precision(v):
    global _USE_DOUBLE_PRECISION
    _USE_DOUBLE_PRECISION = v


def _get_py_float():
    return DTypes.DOUBLE if _USE_DOUBLE_PRECISION else DTypes.FLOAT


class DTypes(Enum):
    NONE = 0
    BLOB = 1
    STRING = 2
    INT8 = 3
    UINT8 = 4
    INT16 = 5
    UINT16 = 6
    INT32 = 7
    UINT32 = 8
    FLOAT = 9
    DOUBLE = 10
    BIGINT = 11


class AutoSuggest:
    pass


class UnknownTypeError(Exception):
    def __init__(self):
        super(UnknownTypeError, self).__init__("Unknown type")


def suggest_type(v) -> DTypes:
    if isinstance(v, float):
        return _get_py_float()
    elif isinstance(v, int):
        if v > 0x7FFFFFFF or v < -2147483648:
            return DTypes.BIGINT
        return DTypes.INT32
    elif isinstance(v, bytes):
        return DTypes.BLOB
    elif isinstance(v, str):
        return DTypes.STRING
    elif v is None:
        return DTypes.NONE
    raise UnknownTypeError
