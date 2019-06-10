from io import BytesIO
from socket import socket
from typing import Union, Type, Callable

from statedb.types import Blob, AutoSuggest, DTypes

Value = Union[int, str, None, Blob]
ValueSrc = Union[Value, socket, BytesIO]
DataType = Union[Type[AutoSuggest], DTypes]
Packer = Callable[[Value], bytes]
Unpacker = Callable[[ValueSrc], Value]
