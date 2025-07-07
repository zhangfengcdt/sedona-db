pub mod exec;
pub mod index;
pub mod once_fut;
pub mod optimizer;
pub mod prep_geom_array;
pub mod spatial_predicate;
pub mod stream;
pub mod utils;

pub use exec::SpatialJoinExec;
pub use optimizer::register_spatial_join_optimizer;

// Re-export option types from sedona-common for convenience
pub use sedona_common::option::*;
