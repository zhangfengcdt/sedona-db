use datafusion::logical_expr::JoinType;

#[derive(Debug, Clone)]
pub struct GpuSpatialJoinConfig {
    /// Join type (Inner, Left, Right, Full)
    pub join_type: JoinType,

    /// Maximum batch size for GPU processing
    pub batch_size: usize,

    /// GPU device ID to use
    pub device_id: i32,

    /// Enable GPU memory pooling
    pub enable_memory_pool: bool,

    /// Maximum GPU memory to use (in bytes)
    pub max_gpu_memory: Option<usize>,
}

impl Default for GpuSpatialJoinConfig {
    fn default() -> Self {
        Self {
            join_type: JoinType::Inner,
            batch_size: 8192,
            device_id: 0,
            enable_memory_pool: true,
            max_gpu_memory: None,
        }
    }
}
