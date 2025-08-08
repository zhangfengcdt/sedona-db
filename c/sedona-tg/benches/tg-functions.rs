use criterion::{criterion_group, criterion_main, Criterion};
use sedona_expr::function_set::FunctionSet;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*, BenchmarkArgs::*};

fn criterion_benchmark(c: &mut Criterion) {
    let mut f = FunctionSet::new();
    for (name, kernel) in sedona_tg::register::scalar_kernels() {
        f.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    benchmark::scalar(
        c,
        &f,
        "tg",
        "st_intersects",
        ArrayScalar(Point, Polygon(10)),
    );
    benchmark::scalar(
        c,
        &f,
        "tg",
        "st_intersects",
        ArrayScalar(Point, Polygon(500)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
