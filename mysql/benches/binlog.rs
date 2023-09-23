use criterion::{black_box, criterion_group, criterion_main, Criterion};

pub fn binlog_benchmark(c: &mut Criterion) {
  c.bench_function("fib 20", |b| {});
}

criterion_group!(benches, binlog_benchmark);
criterion_main!(benches);
