pub mod concurrent_reservation;
pub mod exec;
pub mod index;
pub mod init_once_array;
pub mod once_fut;
pub mod operand_evaluator;
pub mod optimizer;
pub mod refine;
pub mod spatial_predicate;
pub mod stream;
pub mod utils;

pub use exec::SpatialJoinExec;
pub use optimizer::register_spatial_join_optimizer;

// Re-export option types from sedona-common for convenience
pub use sedona_common::option::*;
