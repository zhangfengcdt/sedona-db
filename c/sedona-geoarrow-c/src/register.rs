use sedona_expr::scalar_udf::ScalarKernelRef;

use crate::kernels::{
    st_astext_impl, st_geogfromwkb_impl, st_geogfromwkt_impl, st_geomfromwkb_impl,
    st_geomfromwkt_impl,
};

pub fn scalar_kernels() -> Vec<(&'static str, ScalarKernelRef)> {
    vec![
        ("st_astext", st_astext_impl()),
        ("st_geogfromwkb", st_geogfromwkb_impl()),
        ("st_geogfromwkt", st_geogfromwkt_impl()),
        ("st_geomfromwkb", st_geomfromwkb_impl()),
        ("st_geomfromwkt", st_geomfromwkt_impl()),
    ]
}
