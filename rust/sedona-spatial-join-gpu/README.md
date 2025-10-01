# sedona-spatial-join-gpu

GPU-accelerated spatial join execution for Apache SedonaDB.

## Overview

This package provides GPU-accelerated spatial joins that leverage CUDA for high-performance spatial operations. It integrates with DataFusion's execution engine to accelerate spatial join queries when GPU resources are available.

### Architecture

The GPU spatial join follows a **streaming architecture** that integrates seamlessly with DataFusion:

```
ParquetExec (left) ──┐
                     ├──> GpuSpatialJoinExec ──> Results
ParquetExec (right) ─┘
```

Unlike the CPU-based spatial join, the GPU implementation accepts child ExecutionPlan nodes and reads from their streams, making it composable with any DataFusion operator.

## Features

- **GPU-Accelerated Join**: Leverages CUDA for parallel spatial predicate evaluation
- **Streaming Integration**: Works with DataFusion's existing streaming infrastructure
- **Automatic Fallback**: Falls back to CPU when GPU is unavailable
- **Flexible Configuration**: Configurable device ID, batch size, and memory limits
- **Supported Predicates**: ST_Intersects, ST_Contains, ST_Within, ST_Covers, ST_CoveredBy, ST_Touches, ST_Equals

## Usage

### Prerequisites

- CUDA Toolkit (11.0 or later)
- CUDA-capable GPU
- Build with `--features gpu` flag

### Building

```bash
# Build with GPU support
cargo build --package sedona-spatial-join-gpu --features gpu

# Run tests
cargo test --package sedona-spatial-join-gpu --features gpu
```

### Configuration

GPU spatial join is disabled by default. Enable it via configuration:

```rust
use datafusion::prelude::*;
use sedona_common::option::add_sedona_option_extension;

let config = SessionConfig::new()
    .set_str("sedona.spatial_join.gpu.enable", "true")
    .set_str("sedona.spatial_join.gpu.device_id", "0")
    .set_str("sedona.spatial_join.gpu.batch_size", "8192");

let config = add_sedona_option_extension(config);
let ctx = SessionContext::new_with_config(config);
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `sedona.spatial_join.gpu.enable` | `false` | Enable GPU acceleration |
| `sedona.spatial_join.gpu.device_id` | `0` | GPU device ID to use |
| `sedona.spatial_join.gpu.batch_size` | `8192` | Batch size for processing |
| `sedona.spatial_join.gpu.fallback_to_cpu` | `true` | Fall back to CPU on GPU failure |
| `sedona.spatial_join.gpu.max_memory_mb` | `0` | Max GPU memory in MB (0=unlimited) |
| `sedona.spatial_join.gpu.min_rows_threshold` | `100000` | Minimum rows to use GPU |

## Testing

```bash
# Run unit tests
cargo test --package sedona-spatial-join-gpu

# Run integration tests
cargo test --package sedona-spatial-join-gpu --test integration_test

# Run with GPU feature
cargo test --package sedona-spatial-join-gpu --features gpu
```
