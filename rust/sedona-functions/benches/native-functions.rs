use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_expr::ScalarUDF;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*, BenchmarkArgs};

fn criterion_benchmark(c: &mut Criterion) {
    let f = sedona_functions::register::default_function_set();

    let st_asbinary: ScalarUDF = f.scalar_udf("st_asbinary").unwrap().clone().into();
    let st_astext: ScalarUDF = f.scalar_udf("st_astext").unwrap().clone().into();

    benchmark::scalar(c, &f, "native", "st_astext", Point);
    benchmark::scalar(c, &f, "native", "st_astext", LineString(10));

    benchmark::scalar(c, &f, "native", "st_dimension", Point);
    benchmark::scalar(c, &f, "native", "st_dimension", LineString(10));

    benchmark::scalar(c, &f, "native", "st_geometrytype", Point);
    benchmark::scalar(c, &f, "native", "st_geometrytype", LineString(10));

    benchmark::scalar(
        c,
        &f,
        "native",
        "st_geomfromwkb",
        Transformed(Point.into(), st_asbinary.clone()),
    );
    benchmark::scalar(
        c,
        &f,
        "native",
        "st_geomfromwkt",
        Transformed(Point.into(), st_astext.clone()),
    );
    benchmark::scalar(
        c,
        &f,
        "native",
        "st_geomfromwkt",
        Transformed(LineString(10).into(), st_astext.clone()),
    );

    benchmark::scalar(
        c,
        &f,
        "native",
        "st_point",
        BenchmarkArgs::ArrayArray(Float64(0.0, 100.0), Float64(0.0, 100.0)),
    );

    benchmark::scalar(c, &f, "native", "st_hasz", Point);
    benchmark::scalar(c, &f, "native", "st_hasz", LineString(10));

    benchmark::scalar(c, &f, "native", "st_hasm", Point);
    benchmark::scalar(c, &f, "native", "st_hasm", LineString(10));

    benchmark::scalar(c, &f, "native", "st_x", Point);
    benchmark::scalar(c, &f, "native", "st_y", Point);
    benchmark::scalar(c, &f, "native", "st_z", Point);
    benchmark::scalar(c, &f, "native", "st_m", Point);

    benchmark::aggregate(c, &f, "native", "st_envelope_aggr", Point);
    benchmark::aggregate(c, &f, "native", "st_envelope_aggr", LineString(10));

    benchmark::aggregate(c, &f, "native", "st_analyze_aggr", Point);
    benchmark::aggregate(c, &f, "native", "st_analyze_aggr", LineString(10));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
