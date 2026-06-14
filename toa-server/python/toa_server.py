from collections import namedtuple

import os
_DEBUG = os.getenv('TOA_SERVER_DEBUG') not in (None, '', '0')
del os

_IS_PARENT = 1 << 0
_IS_REFS  = 1 << 1
_IS_VALID = 1 << 2

class Hash:
    __slots__ = '_value',

    def __init__(self, value):
        self._value = bytes(value)
        assert len(self._value) == 32, 'bad length for hash'

    def __repr__(self):
        return self._value.hex()

    def from_hex(s):
        return Hash(bytes.fromhex(s))

class ToaRaw:
    def __init__(self, address):
        from socket import socket, AF_INET, SOCK_DGRAM
        addr, port = address.split(':')
        port = int(port)
        s = socket(AF_INET, SOCK_DGRAM)
        s.bind(('0.0.0.0', 0))
        s.connect((addr, port))
        self._socket = s

    def root(self):
        self._send(1, 0, b'')
        x = self._socket.recv(4 + 32)
        return Hash(x[4:])

    def fetch(self, key: Hash, path = ()):
        if _DEBUG:
            print('fetch', key, path)
        self._send(2, 0, key._value + b''.join(x.to_bytes(4, byteorder='little') for x in path))
        ty, _, x = self._recv()
        if not (ty & _IS_VALID):
            raise UnknownObject(key)
        if ty & _IS_PARENT:
            assert len(x) == 48
            key, bitlen = Hash(x[:32]), int.from_bytes(x[32:], byteorder='little')
            res = (RefsParent if (ty & _IS_REFS) else DataParent)(key, bitlen)
        elif ty & _IS_REFS:
            res = RefsChunk([Hash(x[i:i+32]) for i in range(0, len(x), 32)])
        else:
            res = DataChunk(x)
        if _DEBUG:
            print('got', res)
        return res

    def chunk(self, key: Hash, offset: int):
        if _DEBUG:
            print('chunk', key, offset)
        offset = offset.to_bytes(14, byteorder='little')
        self._send(3, 0, key._value + offset)
        ty, _, x = self._recv()
        if not (ty & _IS_VALID):
            raise UnknownObject(key)
        assert not (ty & _IS_PARENT), "chunk doesn't return parents"
        if ty & _IS_REFS:
            res = RefsChunk([Hash(x[i:i+32]) for i in range(0, len(x), 32)])
        else:
            res = DataChunk(x)
        if _DEBUG:
            print('got', res)
        return res

    def _recv(self):
        msg = self._socket.recv(4 + 8192)
        if _DEBUG:
            print('<- ', msg)
        if len(msg) < 4:
            raise Exception('message too short')
        return msg[0], int.from_bytes(msg[1:4], byteorder='little'), msg[4:]

    def _send(self, cmd, track, data):
        assert 0 <= cmd < 256
        assert 0 <= track < (1 << 24)
        msg = bytes((cmd,)) + track.to_bytes(3, byteorder='little') + data
        if _DEBUG:
            print(' ->', msg)
        self._socket.send(msg)

class Toa:
    __slots__ = '_raw',

    def __init__(self, address):
        self._raw = ToaRaw(address)

    def root(self):
        return self._raw.root()

    def fetch(self, key: Hash, path = (), start = 0, num = -1):
        if start != 0 or num != -1:
            raise NotImplementedError('fetch ranges')
        match self._raw.fetch(key, path):
            case DataChunk(x) | RefsChunk(x):
                return x
            case DataParent(key, bitlen):
                return b''.join(x.data for x in self._fetch_chunks(key, bitlen))
            case RefsParent(key, bitlen):
                return [x for x in x.refs for x in self._fetch_chunks(key, bitlen)]

    def _fetch_chunks(self, key: Hash, bitlen: int) -> iter:
        chunklen = (bitlen + 0xffff) >> 16
        return (self._raw.chunk(key, i) for i in range(chunklen))

DataChunk = namedtuple('DataChunk', ['data'])
RefsChunk = namedtuple('RefsChunk', ['refs'])
DataParent = namedtuple('DataParent', ['key', 'bitlen'])
RefsParent = namedtuple('RefsParent', ['key', 'bitlen'])

class UnknownObject(Exception):
    __slots__ = 'key',

    def __init__(self, key):
        self.key = key
    
    def __str__(self):
        return f'unknown object {self.key}'


def main():
    import sys

    def eprint(*a, **kw):
        print(*a, **kw, file=sys.stderr)

    eprint('shell for exploring toa-server object tree')
    toa = Toa('127.0.0.1:1234')

    def dump(x):
        if type(x) is list:
            for x in x:
                print(x)
            return
        lut = lambda x: chr(x) if 0x20 <= x <= 0x7e else '.'
        N = 32
        for i in range(0, len(x), N):
            s = ''.join(hex(c)[2:].rjust(2, '0') for c in x[i:i+N])
            t = ''.join(lut(c) for c in x[i:i+N])
            print(s.ljust(N * 2, ' ') + '  ' + t)

    def dump_text(x):
        sys.stdout.buffer.write(x)

    def dump_dir(x):
        i = n = 0
        while i < len(x):
            n += 1
            l = x[i]
            i += 1
            print(n, x[i:i+l].decode('utf-8'))
            i += l

    def path(it, dump):
        dump(toa.fetch(toa.root(), it))

    COMMANDS = ('exit', 'get[-text|-dir] <key>', 'path[-text|-dir] [i/j/...]', 'root')

    while True:
        try:
            try:
                eprint('>> ', end='')
                x = input()
            except EOFError:
                eprint('exit')
                break
            except KeyboardInterrupt:
                eprint()
                continue
            match x.split():
                case []:
                    pass
                case ['help']:
                    print('\n'.join(COMMANDS))
                case ['exit']:
                    break
                case ['get', x]:
                    dump(toa.fetch(Hash.from_hex(x)))
                case ['get-text', x]:
                    dump_text(toa.fetch(Hash.from_hex(x)))
                case ['get-dir', x]:
                    dump_dir(toa.fetch(Hash.from_hex(x)))
                case ['path']:
                    path((), dump)
                case ['path-text']:
                    path((), dump_text)
                case ['path-dir']:
                    path((), dump_dir)
                case ['path', x]:
                    path((int(x) for x in x.split('/')), dump)
                case ['path-text', x]:
                    path((int(x) for x in x.split('/')), dump_text)
                case ['path-dir', x]:
                    path((int(x) for x in x.split('/')), dump_dir)
                case ['root']:
                    print(toa.root())
                case _:
                    eprint(f'unknown command {x!r} (try help)')
        except KeyboardInterrupt:
            pass
        except Exception:
            import traceback
            eprint(traceback.format_exc())


if __name__ == '__main__':
    main()

del main
