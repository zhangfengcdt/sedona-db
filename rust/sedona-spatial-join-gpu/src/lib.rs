// Module declarations
pub mod config;
pub mod exec;
pub mod gpu_backend;
pub mod stream;

// Re-exports for convenience
pub use config::{GeometryColumnInfo, GpuSpatialJoinConfig, GpuSpatialPredicate, ParquetFileInfo};
pub use datafusion::logical_expr::JoinType;
pub use exec::GpuSpatialJoinExec;
pub use sedona_libgpuspatial::SpatialPredicate;
pub use stream::GpuSpatialJoinStream;

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
