use arrow_array::ArrayRef;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use sedona_expr::{function_set::FunctionSet, scalar_udf::SedonaScalarUDF};
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_testing::{
    data::test_geoparquet,
    read::{read_geoarrow_data_geometry, TestReadOptions},
};

// These benchmarks are all on ~10,000 geometries
const ARRAY_SIZE: usize = 10000;

pub fn invoke_unary_geometry(udf: &SedonaScalarUDF, batches: &Vec<ArrayRef>) -> Result<usize> {
    let mut out: usize = 0;

    for batch in batches {
        let result = udf.invoke_batch(&[ColumnarValue::Array(batch.clone())], batch.len())?;
        out += result.into_array(batch.len())?.len()
    }

    Ok(out)
}

fn benchmark_st_area(c: &mut Criterion, functions: &FunctionSet) {
    let udf = functions.scalar_udf("st_area").unwrap().clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-poly", &options).unwrap();

    c.bench_function("st_area-wkb_polygons", |b| {
        b.iter(|| invoke_unary_geometry(&udf, &batches))
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // Don't require CI to download asset files for geoarrow-data
    if test_geoparquet("ns-water", "water-poly").is_err() {
        return;
    }

    let mut functions = sedona_functions::register::default_function_set();
    for (name, kernel) in sedona_geo::register::scalar_kernels() {
        functions.add_scalar_udf_kernel(name, kernel).unwrap();
    }

    benchmark_st_area(c, &functions);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
