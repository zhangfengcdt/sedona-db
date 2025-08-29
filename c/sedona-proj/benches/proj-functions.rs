use criterion::{criterion_group, criterion_main, Criterion};
use sedona_expr::function_set::FunctionSet;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*, BenchmarkArgs};

fn criterion_benchmark(c: &mut Criterion) {
    let mut f = FunctionSet::new();
    for (name, kernel) in sedona_proj::register::scalar_kernels() {
        f.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    let args = BenchmarkArgs::ArrayScalarScalar(
        Point,
        String("EPSG:4326".to_string()),
        String("EPSG:3857".to_string()),
    );

    benchmark::scalar(c, &f, "sedona-proj", "st_transform", args);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
