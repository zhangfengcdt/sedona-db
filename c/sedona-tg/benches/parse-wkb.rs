use criterion::{black_box, criterion_group, criterion_main, Criterion};
use geo::{LineString, Point};
use sedona_tg::tg::Geom;

fn criterion_benchmark(c: &mut Criterion) {
    let points = (0..10000)
        .map(|i| Point::new(i as f64, (i + 1) as f64))
        .collect::<Vec<_>>();
    let large_geom = LineString::from(points);
    let mut large_geom_wkb_big_endian = Vec::new();
    wkb::writer::write_geometry(
        &mut large_geom_wkb_big_endian,
        &large_geom,
        wkb::Endianness::BigEndian,
    )
    .unwrap();
    let mut large_geom_wkb_little_endian = Vec::new();
    wkb::writer::write_geometry(
        &mut large_geom_wkb_little_endian,
        &large_geom,
        wkb::Endianness::LittleEndian,
    )
    .unwrap();

    c.bench_function("parse_wkb_big_endian", |b| {
        b.iter(|| {
            let result = Geom::parse_wkb(
                &large_geom_wkb_big_endian,
                sedona_tg::tg::IndexType::Unindexed,
            );
            black_box(result)
        })
    });

    c.bench_function("parse_wkb_little_endian", |b| {
        b.iter(|| {
            let result = Geom::parse_wkb(
                &large_geom_wkb_little_endian,
                sedona_tg::tg::IndexType::Unindexed,
            );
            black_box(result)
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
