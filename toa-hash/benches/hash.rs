use criterion::{
    BenchmarkGroup, Criterion, Throughput, criterion_group, criterion_main,
    measurement::Measurement,
};
use std::hint::black_box;

fn f<M: Measurement>(len: usize, c: &mut BenchmarkGroup<M>) {
    let x = vec![b'x'; len];
    let x = black_box(&*x);
    c.throughput(Throughput::Bytes(len as u64));
    c.bench_function(&format!("len={len}"), |b| b.iter(|| toa_hash::hash(x, &[])));
}

fn bench(c: &mut Criterion) {
    c.bench_function("empty", |b| b.iter(|| toa_hash::hash(b"", &[])));
    let c = &mut c.benchmark_group("hash");
    for n in [
        1,
        8,
        16,
        32,
        64,
        128,
        256,
        1024,
        8000,
        1 << 13,
        1 << 14,
        1 << 15,
        1 << 16,
    ] {
        f(n, c);
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
