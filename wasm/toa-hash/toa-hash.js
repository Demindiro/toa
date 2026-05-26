(async () => {
  const PAGE_SIZE_P2 = 16;
  const PAGE_SIZE = 1 << PAGE_SIZE_P2;
  const mod = await WebAssembly.compileStreaming(fetch("toa_hash_wasm.wasm"))

  return {
    new: async () => {
      const asm = await WebAssembly.instantiate(mod);
      const tbl = {
        beginData: () => asm.exports.begin(1),
        beginRefs: () => asm.exports.begin(2),
        update: data => {
          if (!(data instanceof ArrayBuffer))
            throw new Exception("expect Uint8Array");
          const start = asm.exports.__data_end;
          const mem_end = asm.exports.memory.buffer.byteLength;
          const need = data.byteLength - (mem_end - start)
          if (need > 0) {
            const page_num = (need + PAGE_SIZE) >> PAGE_SIZE_P2;
            asm.exports.memory.grow(page_num)
          }
          // there's probably a sort of memcpy but I can't find it so ~lol~
          const host = new Uint8Array(data);
          const modul = new Uint8Array(asm.exports.memory.buffer);
          for (let i = 0; i < host.length; i++)
            modul[start + i] = host[i]
          asm.exports.update(start, host.length);
        },
        end: () => {
          asm.exports.end()
          const x = asm.exports.HASH.value
          return (new Uint8Array(asm.exports.memory.buffer)).slice(x, x + 32)
        },
        hashData: data => {
          tbl.beginData();
          tbl.update(data);
          return tbl.end();
        },
        hashRefs: refs => {
          tbl.beginRefs();
          tbl.update(refs);
          return tbl.end();
        },
      };
      return tbl;
    },
  };
})()
