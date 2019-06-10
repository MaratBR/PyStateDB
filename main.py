import time

from statedb import *


conn = Connection('127.0.0.1', 3456)
conn.connect()
conn.listen_thread()
s = conn.storage
#s.request_value('Int8')
#s.update_all()

s['Int8'] = DTypes.INT8, 42
s['UInt8'] = DTypes.UINT8, 42
s['Int16'] = DTypes.INT16, 42
s['UInt16'] = DTypes.UINT16, 42
s['Int32'] = DTypes.INT32, 42
s['UInt32'] = DTypes.UINT32, 42

s['Str'] = DTypes.STRING, 'asdafrtgfhwesrdfgasedfgaesgdfrg'
s['Bytes'] = DTypes.BLOB, b'efgrdhjkldsfghjkjgwehd'


del s['Str']
time.sleep(4)

print(s)
print(s._types)

time.sleep(100000)




















