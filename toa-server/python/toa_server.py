from collections import namedtuple

import os
_DEBUG = os.getenv('TOA_SERVER_DEBUG') not in (None, '', '0')
del os

_IS_PAIR  = 1 << 0
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

class Toa:
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

    def fetch(self, key: Hash):
        self._send(2, 0, key._value)
        ty, _, x = self._recv()
        if not (ty & _IS_VALID):
            raise UnknownObject(key)
        if ty & _IS_PAIR:
            assert len(x) == 80
            l, h, bitlen = Hash(x[:32]), Hash(x[32:64]), x[64:]
            res = (RefsPair if (ty & _IS_REFS) else DataPair)(l, h)
        elif ty & _IS_REFS:
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

DataChunk = namedtuple('DataChunk', ['data'])
RefsChunk = namedtuple('RefsChunk', ['refs'])
DataPair = namedtuple('DataPair', ['l', 'h'])
RefsPair = namedtuple('RefsPair', ['l', 'h'])

class UnknownObject(Exception):
    __slots__ = 'key',

    def __init__(self, key):
        self.key = key
    
    def __str__(self):
        return f'unknown object {self.key}'


def main():
    print('shell for exploring toa-server object tree')
    toa = Toa('127.0.0.1:1234')

    def dump(x):
        match toa.fetch(x):
            case DataChunk(data):
                lut = lambda x: chr(x) if 0x20 <= x <= 0x7e else '.'
                N = 32
                for i in range(0, len(data), N):
                    s = ''.join(hex(c)[2:].rjust(2, '0') for c in data[i:i+N])
                    t = ''.join(lut(c) for c in data[i:i+N])
                    print(s.ljust(N * 2, ' ') + '  ' + t)
            case RefsChunk(refs):
                for x in refs:
                    print(x)
            case DataPair(l, h) | RefsPair(l, h):
                dump(l)
                dump(h)
            case _ as x:
                raise Exception(f'wtf is this {x!r}')

    def dump_text(x):
        last_nl = False
        def f(x):
            nonlocal last_nl
            match toa.fetch(x):
                case DataChunk(data):
                    print(data.decode('utf-8'), end='')
                    last_nl = data and data[-1] == ord('\n')
                case DataPair(l, h):
                    f(l)
                    f(h)
                case RefsChunk() | RefsPair():
                    raise Exception("can't dump refs")
                case _ as x:
                    raise Exception(f'wtf is this {x!r}')
        f(x)
        if not last_nl:
            print('\x1b[7m%\x1b[0m')

    def path(it, dump):
        cur = toa.root()
        for x in it:
            while True:
                match toa.fetch(cur):
                    case DataChunk() | DataPair():
                        raise Exception('encountered data')
                    case RefsChunk(refs):
                        cur = refs[x]
                        break
                    case RefsPair(l, h):
                        assert 0, 'todo refs pair'
        dump(cur)

    COMMANDS = ('exit', 'get[-text] <key>', 'path[-text] [i/j/...]', 'root')

    while True:
        try:
            try:
                x = input('>> ')
            except EOFError:
                print('exit')
                break
            except KeyboardInterrupt:
                print()
                continue
            match x.split():
                case ['']:
                    pass
                case ['help']:
                    print('\n'.join(COMMANDS))
                case ['exit']:
                    break
                case ['get', x]:
                    dump(Hash.from_hex(x))
                case ['get', x]:
                    dump_text(Hash.from_hex(x))
                case ['path']:
                    path((), dump)
                case ['path-text']:
                    path((), dump_text)
                case ['path', x]:
                    path((int(x) for x in x.split('/')), dump)
                case ['path-text', x]:
                    path((int(x) for x in x.split('/')), dump_text)
                case ['root']:
                    print(toa.root())
                case _:
                    print(f'unknown command {x!r} (try help)')
        except KeyboardInterrupt:
            pass
        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    main()

del main
