use arrow_array::ArrayRef;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use sedona_expr::scalar_udf::SedonaScalarUDF;
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_testing::{
    data::test_geoparquet,
    read::{read_geoarrow_data_geometry, TestReadOptions},
};

// These benchmarks are all on ~250,000 points
const ARRAY_SIZE: usize = 250000;

pub fn invoke_unary_geometry(udf: &SedonaScalarUDF, batches: &Vec<ArrayRef>) -> Result<usize> {
    let mut out: usize = 0;

    for batch in batches {
        let result = udf.invoke_batch(&[ColumnarValue::Array(batch.clone())], batch.len())?;
        out += result.into_array(batch.len())?.len()
    }

    Ok(out)
}

pub fn benchmark_st_x(c: &mut Criterion) {
    let udf = sedona_functions::register::default_function_set()
        .scalar_udf("st_x")
        .unwrap()
        .clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();

    c.bench_function("st_x-wkb_points", |b| {
        b.iter(|| invoke_unary_geometry(&udf, &batches))
    });
}

fn benchmark_st_asbinary(c: &mut Criterion) {
    let udf = sedona_functions::register::default_function_set()
        .scalar_udf("st_asbinary")
        .unwrap()
        .clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();

    c.bench_function("st_asbinary-wkb_points", |b| {
        b.iter(|| invoke_unary_geometry(&udf, &batches))
    });
}

pub fn benchmark_st_astext(c: &mut Criterion) {
    let udf = sedona_functions::register::default_function_set()
        .scalar_udf("st_astext")
        .unwrap()
        .clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();

    c.bench_function("st_astext-wkb_points", |b| {
        b.iter(|| invoke_unary_geometry(&udf, &batches))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // Don't require CI to download asset files for geoarrow-data
    if test_geoparquet("ns-water", "water-junc").is_err() {
        return;
    }

    benchmark_st_x(c);
    benchmark_st_asbinary(c);
    benchmark_st_astext(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
