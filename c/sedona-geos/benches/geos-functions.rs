use criterion::{criterion_group, criterion_main, Criterion};
use sedona_expr::function_set::FunctionSet;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*};

fn criterion_benchmark(c: &mut Criterion) {
    let mut f = FunctionSet::new();
    for (name, kernel) in sedona_geos::register::scalar_kernels() {
        f.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    benchmark::scalar(c, &f, "geos", "st_area", Polygon(10));
    benchmark::scalar(c, &f, "geos", "st_area", Polygon(500));

    benchmark::scalar(c, &f, "geos", "st_length", LineString(10));
    benchmark::scalar(c, &f, "geos", "st_length", LineString(500));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
