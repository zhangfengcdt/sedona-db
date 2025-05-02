use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_testing::{
    data::test_geoparquet,
    read::{read_geoarrow_data_geometry, TestReadOptions},
};

// These benchmarks are all on ~250,000 points
const ARRAY_SIZE: usize = 250000;

pub fn benchmark_st_point(c: &mut Criterion) {
    let functions = sedona_functions::register::default_function_set();
    let st_x = functions.scalar_udf("st_x").unwrap();
    let st_y = functions.scalar_udf("st_y").unwrap();
    let udf = functions.scalar_udf("st_point").unwrap().clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();
    let xs = batches
        .iter()
        .map(|b| st_x.invoke_batch(&[ColumnarValue::Array(b.clone())], b.len()))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let ys = batches
        .iter()
        .map(|b| st_y.invoke_batch(&[ColumnarValue::Array(b.clone())], b.len()))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let sizes = batches.iter().map(|batch| batch.len()).collect::<Vec<_>>();

    c.bench_function("st_point", |b| {
        b.iter(|| {
            let xs = xs.clone();
            let ys = ys.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[xs[i].clone(), ys[i].clone()], sizes[i])
                    .unwrap();
            }
        })
    });
}

pub fn benchmark_st_geomfromwkb(c: &mut Criterion) {
    let functions = sedona_functions::register::default_function_set();
    let st_asbinary = functions.scalar_udf("st_asbinary").unwrap();
    let udf = functions.scalar_udf("st_geomfromwkb").unwrap().clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();
    let binary = batches
        .iter()
        .map(|b| st_asbinary.invoke_batch(&[ColumnarValue::Array(b.clone())], b.len()))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let sizes = batches.iter().map(|batch| batch.len()).collect::<Vec<_>>();

    c.bench_function("st_geomfromwkb", |b| {
        b.iter(|| {
            let binary = binary.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[binary[i].clone()], sizes[i]).unwrap();
            }
        })
    });
}

pub fn benchmark_st_geomfromwkt(c: &mut Criterion) {
    let functions = sedona_functions::register::default_function_set();
    let st_astext = functions.scalar_udf("st_astext").unwrap();
    let udf = functions.scalar_udf("st_geomfromwkt").unwrap().clone();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();
    let text = batches
        .iter()
        .map(|b| st_astext.invoke_batch(&[ColumnarValue::Array(b.clone())], b.len()))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let sizes = batches.iter().map(|batch| batch.len()).collect::<Vec<_>>();

    c.bench_function("st_geomfromwkt", |b| {
        b.iter(|| {
            let text = text.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[text[i].clone()], sizes[i]).unwrap();
            }
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // Don't require CI to download asset files for geoarrow-data
    if test_geoparquet("ns-water", "water-junc").is_err() {
        return;
    }

    benchmark_st_point(c);
    benchmark_st_geomfromwkb(c);
    benchmark_st_geomfromwkt(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
