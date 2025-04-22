use datafusion::prelude::SessionContext;
use sedona_functions::{
    st_asbinary::st_asbinary_udf,
    st_astext::st_astext_udf,
    st_geomfromwkb::{st_geogfromwkb_udf, st_geomfromwkb_udf},
    st_geomfromwkt::{st_geogfromwkt_udf, st_geomfromwkt_udf},
    st_point::{st_geogpoint_udf, st_point_udf},
    st_xy::{st_x_udf, st_y_udf},
};

/// Register Sedona user-defined scalar functions to a DataFusion [`SessionContext`]
pub fn register_sedona_scalar_udfs(ctx: &SessionContext) {
    ctx.register_udf(st_asbinary_udf().into());
    ctx.register_udf(st_astext_udf().into());
    ctx.register_udf(st_geogfromwkb_udf().into());
    ctx.register_udf(st_geogfromwkt_udf().into());
    ctx.register_udf(st_geogpoint_udf().into());
    ctx.register_udf(st_geomfromwkb_udf().into());
    ctx.register_udf(st_geomfromwkt_udf().into());
    ctx.register_udf(st_point_udf().into());
    ctx.register_udf(st_x_udf().into());
    ctx.register_udf(st_y_udf().into());
}
