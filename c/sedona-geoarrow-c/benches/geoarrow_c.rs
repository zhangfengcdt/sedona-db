use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::Result;
use datafusion_expr::ColumnarValue;
use sedona::ffi::FFI_SedonaScalarKernel;
use sedona_geoarrow_c::kernels::{st_astext_impl, st_geomfromwkb_impl, st_geomfromwkt_impl};
use sedona_schema::datatypes::WKB_GEOMETRY;
use sedona_testing::{
    data::test_geoparquet,
    read::{read_geoarrow_data_geometry, TestReadOptions},
};

// These benchmarks are all on ~250,000 points
const ARRAY_SIZE: usize = 250000;

pub fn benchmark_st_geomfromwkb(c: &mut Criterion) {
    let mut functions = sedona_functions::register::default_function_set();
    functions
        .add_scalar_udf_kernel("st_geomfromwkb", st_geomfromwkb_impl())
        .unwrap();

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

    c.bench_function("geoarrow_c-st_geomfromwkb", |b| {
        b.iter(|| {
            let binary = binary.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[binary[i].clone()], sizes[i]).unwrap();
            }
        })
    });
}

pub fn benchmark_st_geomfromwkt(c: &mut Criterion) {
    let mut functions = sedona_functions::register::default_function_set();
    functions
        .add_scalar_udf_kernel("st_geomfromwkt", st_geomfromwkt_impl())
        .unwrap();

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

    c.bench_function("geoarrow_c-st_geomfromwkt", |b| {
        b.iter(|| {
            let text = text.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[text[i].clone()], sizes[i]).unwrap();
            }
        })
    });
}

pub fn benchmark_st_astext(c: &mut Criterion) {
    let mut functions = sedona_functions::register::default_function_set();
    functions
        .add_scalar_udf_kernel("st_astext", st_astext_impl())
        .unwrap();
    let udf = functions.scalar_udf("st_astext").unwrap();

    let options = TestReadOptions::new(WKB_GEOMETRY).with_output_size(ARRAY_SIZE);
    let batches = read_geoarrow_data_geometry("ns-water", "water-junc", &options).unwrap();
    let sizes = batches.iter().map(|batch| batch.len()).collect::<Vec<_>>();

    c.bench_function("geoarrow_c-st_astext-wkb_points", |b| {
        b.iter(|| {
            let mut out: usize = 0;

            for i in 0..batches.len() {
                let result = udf
                    .invoke_batch(&[ColumnarValue::Array(batches[i].clone())], sizes[i])
                    .unwrap();
                out += result.into_array(sizes[i]).unwrap().len()
            }

            out
        })
    });
}

// Checks the overhead associated with calling one of the cheapest functions
// we have via ffi.
pub fn benchmark_st_geomfromwkb_via_ffi(c: &mut Criterion) {
    let mut functions = sedona_functions::register::default_function_set();
    let kernel_ffi = FFI_SedonaScalarKernel::from(st_geomfromwkb_impl());
    functions
        .add_scalar_udf_kernel("st_geomfromwkb", kernel_ffi.try_into().unwrap())
        .unwrap();

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

    c.bench_function("geoarrow_c-st_geomfromwkb_via_ffi", |b| {
        b.iter(|| {
            let binary = binary.clone();
            for i in 0..batches.len() {
                udf.invoke_batch(&[binary[i].clone()], sizes[i]).unwrap();
            }
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    // Don't require CI to download asset files for geoarrow-data
    if test_geoparquet("ns-water", "water-junc").is_err() {
        return;
    }

    benchmark_st_geomfromwkb(c);
    benchmark_st_geomfromwkt(c);
    benchmark_st_astext(c);
    benchmark_st_geomfromwkb_via_ffi(c);
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
