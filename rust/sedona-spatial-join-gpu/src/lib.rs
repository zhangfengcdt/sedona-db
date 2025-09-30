pub mod gpu_spatial_join;

pub use datafusion::logical_expr::JoinType;
pub use gpu_spatial_join::{GpuSpatialJoinConfig, GpuSpatialJoinExec};
pub use sedona_libgpuspatial::SpatialPredicate;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("GPU initialization error: {0}")]
    GpuInit(String),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("GPU spatial operation error: {0}")]
    GpuSpatial(String),
}

pub type Result<T> = std::result::Result<T, Error>;
