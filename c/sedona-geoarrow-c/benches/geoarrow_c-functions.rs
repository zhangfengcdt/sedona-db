use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_expr::ScalarUDF;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*};

fn criterion_benchmark(c: &mut Criterion) {
    let mut f = sedona_functions::register::default_function_set();
    for (name, kernel) in sedona_geoarrow_c::register::scalar_kernels() {
        f.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    let st_asbinary: ScalarUDF = f.scalar_udf("st_asbinary").unwrap().clone().into();
    let st_astext: ScalarUDF = f.scalar_udf("st_astext").unwrap().clone().into();

    benchmark::scalar(c, &f, "geoarrow_c", "st_astext", Point);
    benchmark::scalar(c, &f, "geoarrow_c", "st_astext", LineString(10));
    benchmark::scalar(
        c,
        &f,
        "geoarrow_c",
        "st_geomfromwkb",
        Transformed(Point.into(), st_asbinary.clone()),
    );
    benchmark::scalar(
        c,
        &f,
        "geoarrow_c",
        "st_geomfromwkt",
        Transformed(Point.into(), st_astext.clone()),
    );
    benchmark::scalar(
        c,
        &f,
        "geoarrow_c",
        "st_geomfromwkt",
        Transformed(LineString(10).into(), st_astext.clone()),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
