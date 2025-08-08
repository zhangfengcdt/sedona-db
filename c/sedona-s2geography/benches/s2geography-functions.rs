use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_expr::ScalarUDF;
use sedona_expr::{
    function_set::FunctionSet,
    scalar_udf::{ArgMatcher, SedonaScalarUDF, SimpleSedonaScalarKernel},
};
use sedona_schema::datatypes::WKB_GEOGRAPHY;
use sedona_testing::benchmark_util::{benchmark, BenchmarkArgSpec::*, BenchmarkArgs};

fn criterion_benchmark(c: &mut Criterion) {
    let mut f = FunctionSet::new();
    for (name, kernel) in sedona_s2geography::register::scalar_kernels() {
        f.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    // Single geometry functions
    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_area",
        Transformed(Polygon(10).into(), to_geography()),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_length",
        Transformed(LineString(10).into(), to_geography()),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_centroid",
        Transformed(Polygon(10).into(), to_geography()),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_convexhull",
        Transformed(Polygon(10).into(), to_geography()),
    );

    // Binary geometry functions
    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_distance",
        BenchmarkArgs::ArrayScalar(
            Transformed(Point.into(), to_geography()),
            Transformed(Point.into(), to_geography()),
        ),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_closestpoint",
        BenchmarkArgs::ArrayScalar(
            Transformed(LineString(10).into(), to_geography()),
            Transformed(LineString(10).into(), to_geography()),
        ),
    );
    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_intersection",
        BenchmarkArgs::ArrayScalar(
            Transformed(Polygon(10).into(), to_geography()),
            Transformed(Polygon(10).into(), to_geography()),
        ),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_union",
        BenchmarkArgs::ArrayScalar(
            Transformed(Polygon(10).into(), to_geography()),
            Transformed(Polygon(10).into(), to_geography()),
        ),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_difference",
        BenchmarkArgs::ArrayScalar(
            Transformed(Polygon(10).into(), to_geography()),
            Transformed(Polygon(10).into(), to_geography()),
        ),
    );

    // Predicate functions
    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_contains",
        BenchmarkArgs::ScalarArray(
            Transformed(Polygon(10).into(), to_geography()),
            Transformed(Point.into(), to_geography()),
        ),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_intersects",
        BenchmarkArgs::ArrayScalar(
            Transformed(Point.into(), to_geography()),
            Transformed(Polygon(10).into(), to_geography()),
        ),
    );

    benchmark::scalar(
        c,
        &f,
        "s2geography",
        "st_equals",
        BenchmarkArgs::ArrayScalar(
            Transformed(Polygon(10).into(), to_geography()),
            Transformed(Polygon(10).into(), to_geography()),
        ),
    );
}

fn to_geography() -> ScalarUDF {
    let kernel = SimpleSedonaScalarKernel::new_ref(
        ArgMatcher::new(vec![ArgMatcher::is_geometry()], WKB_GEOGRAPHY),
        Arc::new(move |_, args| args[0].cast_to(WKB_GEOGRAPHY.storage_type(), None)),
    );
    SedonaScalarUDF::from_kernel("geog", kernel).into()
}

criterion_group! {
    name = benches;
    // These are currently very slow, so only run 10 samples
    config = Criterion::default().sample_size(10);
    targets = criterion_benchmark
}
criterion_main!(benches);
