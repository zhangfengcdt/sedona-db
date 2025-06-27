use arrow_array::{ArrayRef, RecordBatch};
use arrow_array::{BinaryArray, BinaryViewArray};
use arrow_array::{Float64Array, Int32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::Result;
use geo_types::{Coord, Geometry, LineString, Point, Polygon, Rect};
use rand::distributions::Uniform;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sedona_geometry::types::GeometryTypeId;
use sedona_schema::datatypes::{SedonaType, WKB_GEOMETRY};
use std::sync::Arc;

/// Generate random geometry WKB bytes based on the geometry type
fn generate_random_geometry<R: rand::Rng>(
    rng: &mut R,
    geom_type: &GeometryTypeId,
    bounds: &Rect,
    size_range: (f64, f64),
) -> Vec<u8> {
    let geometry = match geom_type {
        GeometryTypeId::Point => {
            // Generate random points within the specified bounds
            let x_dist = Uniform::new(bounds.min().x, bounds.max().x);
            let y_dist = Uniform::new(bounds.min().y, bounds.max().y);
            let x = rng.sample(x_dist);
            let y = rng.sample(y_dist);
            Geometry::Point(Point::new(x, y))
        }
        GeometryTypeId::Polygon => {
            // Generate random diamond polygons (rotated squares)
            let size_dist = Uniform::new(size_range.0, size_range.1);
            let half_size = rng.sample(size_dist);

            // Ensure diamond fits within bounds by constraining center position
            let center_x_dist =
                Uniform::new(bounds.min().x + half_size, bounds.max().x - half_size);
            let center_y_dist =
                Uniform::new(bounds.min().y + half_size, bounds.max().y - half_size);
            let center_x = rng.sample(center_x_dist);
            let center_y = rng.sample(center_y_dist);

            // Create a diamond polygon (square rotated 45 degrees)
            let coords = vec![
                Coord {
                    x: center_x,
                    y: center_y + half_size,
                }, // Top
                Coord {
                    x: center_x + half_size,
                    y: center_y,
                }, // Right
                Coord {
                    x: center_x,
                    y: center_y - half_size,
                }, // Bottom
                Coord {
                    x: center_x - half_size,
                    y: center_y,
                }, // Left
                Coord {
                    x: center_x,
                    y: center_y + half_size,
                }, // Close the ring
            ];
            let line_string = LineString::from(coords);
            let polygon = Polygon::new(line_string, vec![]);
            Geometry::Polygon(polygon)
        }
        _ => {
            // For other geometry types, default to generating a point
            let x_dist = Uniform::new(bounds.min().x, bounds.max().x);
            let y_dist = Uniform::new(bounds.min().y, bounds.max().y);
            let x = rng.sample(x_dist);
            let y = rng.sample(y_dist);
            Geometry::Point(Point::new(x, y))
        }
    };

    // Convert geometry to WKB
    let mut out: Vec<u8> = vec![];
    wkb::writer::write_geometry(&mut out, &geometry, Default::default()).unwrap();
    out
}

/// Builder for generating test data partitions with random geometries.
///
/// This builder allows you to create deterministic test datasets with configurable
/// geometry types, data distribution, and partitioning for testing spatial operations.
/// The generated data includes:
/// - `id`: Unique integer identifier for each row
/// - `dist`: Random floating-point distance value (0.0 to 100.0)
/// - `geometry`: Random geometry data in the specified format (WKB or WKB View)
///
/// # Example
///
/// ```rust
/// use sedona_testing::datagen::RandomPartitionedDataBuilder;
/// use sedona_geometry::types::GeometryTypeId;
/// use geo_types::{Coord, Rect};
///
/// let (schema, partitions) = RandomPartitionedDataBuilder::new()
///     .seed(42)
///     .num_partitions(4)
///     .rows_per_batch(1000)
///     .geometry_type(GeometryTypeId::Polygon)
///     .bounds(Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 }))
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct RandomPartitionedDataBuilder {
    seed: u64,
    num_partitions: usize,
    batches_per_partition: usize,
    rows_per_batch: usize,
    geom_type: GeometryTypeId,
    sedona_type: SedonaType,
    bounds: Rect,
    size_range: (f64, f64),
    null_rate: f64,
}

impl Default for RandomPartitionedDataBuilder {
    fn default() -> Self {
        Self {
            seed: 42,
            num_partitions: 1,
            batches_per_partition: 1,
            rows_per_batch: 10,
            geom_type: GeometryTypeId::Point,
            sedona_type: WKB_GEOMETRY,
            bounds: Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 }),
            size_range: (1.0, 10.0),
            null_rate: 0.0,
        }
    }
}

impl RandomPartitionedDataBuilder {
    /// Creates a new `RandomPartitionedDataBuilder` with default values.
    ///
    /// Default configuration:
    /// - seed: 42 (for deterministic results)
    /// - num_partitions: 1
    /// - batches_per_partition: 1
    /// - rows_per_batch: 10
    /// - geometry_type: Point
    /// - bounds: (0,0) to (100,100)
    /// - size_range: 1.0 to 10.0
    /// - null_rate: 0.0 (no nulls)
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the random seed for deterministic data generation.
    ///
    /// Using the same seed will produce identical datasets, which is useful
    /// for reproducible tests.
    ///
    /// # Arguments
    ///
    /// * `seed` - The random seed value
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Sets the number of data partitions to generate.
    ///
    /// Each partition contains multiple batches of data. This is useful for
    /// testing distributed processing scenarios.
    ///
    /// # Arguments
    ///
    /// * `num_partitions` - Number of partitions to create
    pub fn num_partitions(mut self, num_partitions: usize) -> Self {
        self.num_partitions = num_partitions;
        self
    }

    /// Sets the number of batches per partition.
    ///
    /// Each batch is a `RecordBatch` containing the specified number of rows.
    ///
    /// # Arguments
    ///
    /// * `batches_per_partition` - Number of batches in each partition
    pub fn batches_per_partition(mut self, batches_per_partition: usize) -> Self {
        self.batches_per_partition = batches_per_partition;
        self
    }

    /// Sets the number of rows per batch.
    ///
    /// This determines the size of each `RecordBatch` that will be generated.
    ///
    /// # Arguments
    ///
    /// * `rows_per_batch` - Number of rows in each batch
    pub fn rows_per_batch(mut self, rows_per_batch: usize) -> Self {
        self.rows_per_batch = rows_per_batch;
        self
    }

    /// Sets the type of geometry to generate.
    ///
    /// Currently supports:
    /// - `GeometryTypeId::Point`: Random points within the specified bounds
    /// - `GeometryTypeId::Polygon`: Random diamond-shaped polygons
    /// - Other types default to point generation
    ///
    /// # Arguments
    ///
    /// * `geom_type` - The geometry type to generate
    pub fn geometry_type(mut self, geom_type: GeometryTypeId) -> Self {
        self.geom_type = geom_type;
        self
    }

    /// Sets the Sedona data type for the geometry column.
    ///
    /// This determines how the geometry data is stored (e.g., WKB or WKB View).
    ///
    /// # Arguments
    ///
    /// * `sedona_type` - The Sedona type for geometry storage
    pub fn sedona_type(mut self, sedona_type: SedonaType) -> Self {
        self.sedona_type = sedona_type;
        self
    }

    /// Sets the spatial bounds for geometry generation.
    ///
    /// All generated geometries will be positioned within these bounds.
    /// For polygons, the bounds are used to ensure the entire polygon fits within the area.
    ///
    /// # Arguments
    ///
    /// * `bounds` - Rectangle defining the spatial bounds (min_x, min_y, max_x, max_y)
    pub fn bounds(mut self, bounds: Rect) -> Self {
        self.bounds = bounds;
        self
    }

    /// Sets the size range for generated geometries.
    ///
    /// For polygons, this controls the radius of the generated shapes.
    /// For points, this parameter is not used.
    ///
    /// # Arguments
    ///
    /// * `size_range` - Tuple of (min_size, max_size) for geometry dimensions
    pub fn size_range(mut self, size_range: (f64, f64)) -> Self {
        self.size_range = size_range;
        self
    }

    /// Sets the rate of null values in the geometry column.
    ///
    /// # Arguments
    ///
    /// * `null_rate` - Fraction of rows that should have null geometry (0.0 to 1.0)
    pub fn null_rate(mut self, null_rate: f64) -> Self {
        self.null_rate = null_rate;
        self
    }

    /// Builds the random partitioned dataset with the configured parameters.
    ///
    /// Generates a deterministic dataset based on the seed and configuration.
    /// The resulting schema contains three columns:
    /// - `id`: Int32 - Unique sequential identifier for each row
    /// - `dist`: Float64 - Random distance value between 0.0 and 100.0
    /// - `geometry`: SedonaType - Random geometry data (WKB or WKB View format)
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - `SchemaRef`: Arrow schema for the generated data
    /// - `Vec<Vec<RecordBatch>>`: Vector of partitions, each containing a vector of record batches
    ///
    /// # Errors
    ///
    /// Returns a `datafusion_common::Result` error if:
    /// - RecordBatch creation fails
    /// - Array conversion fails
    /// - Schema creation fails
    pub fn build(self) -> Result<(SchemaRef, Vec<Vec<RecordBatch>>)> {
        // Create a seeded random number generator for deterministic results
        let mut rng = StdRng::seed_from_u64(self.seed);

        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("dist", DataType::Float64, false),
            Field::new("geometry", self.sedona_type.clone().into(), true),
        ]));

        let mut result = Vec::with_capacity(self.num_partitions);

        for partition_idx in 0..self.num_partitions {
            let mut partition_batches = Vec::with_capacity(self.batches_per_partition);

            for batch_idx in 0..self.batches_per_partition {
                // Generate IDs - make them unique across partitions and batches
                let id_start =
                    (partition_idx * self.batches_per_partition + batch_idx) * self.rows_per_batch;
                let ids: Vec<i32> = (0..self.rows_per_batch)
                    .map(|i| (id_start + i) as i32)
                    .collect();

                // Generate random distances between 0.0 and 100.0
                let distance_dist = Uniform::new(0.0, 100.0);
                let distances: Vec<f64> = (0..self.rows_per_batch)
                    .map(|_| rng.sample(distance_dist))
                    .collect();

                // Generate random geometries based on the geometry type
                let wkb_geometries: Vec<Option<Vec<u8>>> = (0..self.rows_per_batch)
                    .map(|_| {
                        if rng.sample(Uniform::new(0.0, 1.0)) < self.null_rate {
                            None
                        } else {
                            Some(generate_random_geometry(
                                &mut rng,
                                &self.geom_type,
                                &self.bounds,
                                self.size_range,
                            ))
                        }
                    })
                    .collect();

                // Create Arrow arrays
                let id_array = Arc::new(Int32Array::from(ids));
                let dist_array = Arc::new(Float64Array::from(distances));
                let geometry_array = create_wkb_array(wkb_geometries, &self.sedona_type);

                // Create RecordBatch
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![id_array, dist_array, geometry_array],
                )?;

                partition_batches.push(batch);
            }

            result.push(partition_batches);
        }

        Ok((schema, result))
    }
}

/// Create an ArrayRef from a vector of WKB bytes based on the sedona type
fn create_wkb_array(wkb_values: Vec<Option<Vec<u8>>>, sedona_type: &SedonaType) -> ArrayRef {
    let storage_array: ArrayRef = match sedona_type {
        SedonaType::Wkb(_, _) => Arc::new(BinaryArray::from_iter(wkb_values)),
        SedonaType::WkbView(_, _) => Arc::new(BinaryViewArray::from_iter(wkb_values)),
        _ => panic!("create_wkb_array not implemented for {sedona_type:?}"),
    };
    sedona_type.wrap_array(&storage_array).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use geo_types::Coord;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn test_generate_random_geometry_produces_valid_wkb() {
        let bounds = Rect::new(Coord { x: 10.0, y: 10.0 }, Coord { x: 90.0, y: 90.0 });
        let size_range = (1.0, 10.0);

        // Test both Point and Polygon geometry types
        let test_cases = vec![
            (GeometryTypeId::Point, 42, 100, 20, 50), // (type, seed, iterations, min_size, max_size)
            (GeometryTypeId::Polygon, 123, 50, 80, 200),
        ];

        for (geom_type, seed, iterations, min_size, max_size) in test_cases {
            let mut rng = StdRng::seed_from_u64(seed);

            for _ in 0..iterations {
                let wkb_bytes = generate_random_geometry(&mut rng, &geom_type, &bounds, size_range);

                // Verify WKB is not empty and has reasonable size
                assert!(!wkb_bytes.is_empty());
                assert!(
                    wkb_bytes.len() >= min_size,
                    "WKB size {} is smaller than expected minimum {} for {:?}",
                    wkb_bytes.len(),
                    min_size,
                    geom_type
                );
                assert!(
                    wkb_bytes.len() <= max_size,
                    "WKB size {} is larger than expected maximum {} for {:?}",
                    wkb_bytes.len(),
                    max_size,
                    geom_type
                );

                // Verify WKB can be parsed without error
                wkb::reader::read_wkb(&wkb_bytes).unwrap();
            }
        }
    }

    #[test]
    fn test_generate_random_geometry_deterministic() {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });
        let size_range = (1.0, 10.0);

        let geom_types = [GeometryTypeId::Point, GeometryTypeId::Polygon];

        // Generate with same seed twice
        let mut rng1 = StdRng::seed_from_u64(42);
        let mut rng2 = StdRng::seed_from_u64(42);

        for geom_type in geom_types {
            let wkb1 = generate_random_geometry(&mut rng1, &geom_type, &bounds, size_range);
            let wkb2 = generate_random_geometry(&mut rng2, &geom_type, &bounds, size_range);

            // Should generate identical results
            assert_eq!(wkb1, wkb2);
        }
    }

    #[test]
    fn test_random_partitioned_data_builder_build_basic() {
        let (schema, partitions) = RandomPartitionedDataBuilder::new()
            .num_partitions(2)
            .batches_per_partition(3)
            .rows_per_batch(4)
            .null_rate(0.0) // No nulls for easier testing
            .build()
            .unwrap();

        // Verify schema
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(schema.field(1).name(), "dist");
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
        assert_eq!(schema.field(2).name(), "geometry");

        // Verify partitions structure
        assert_eq!(partitions.len(), 2); // num_partitions

        for partition in &partitions {
            assert_eq!(partition.len(), 3); // batches_per_partition

            for batch in partition {
                assert_eq!(batch.num_rows(), 4); // rows_per_batch
                assert_eq!(batch.num_columns(), 3);
            }
        }
    }

    #[test]
    fn test_random_partitioned_data_builder_unique_ids() {
        let (_, partitions) = RandomPartitionedDataBuilder::new()
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(3)
            .build()
            .unwrap();

        let mut all_ids = Vec::new();

        for partition in &partitions {
            for batch in partition {
                let id_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for i in 0..id_array.len() {
                    all_ids.push(id_array.value(i));
                }
            }
        }

        // Verify all IDs are unique
        all_ids.sort();
        for i in 1..all_ids.len() {
            assert_ne!(
                all_ids[i - 1],
                all_ids[i],
                "Found duplicate ID: {}",
                all_ids[i]
            );
        }

        // Verify IDs are sequential starting from 0
        for (i, &id) in all_ids.iter().enumerate() {
            assert_eq!(id, i as i32);
        }
    }

    #[test]
    fn test_random_partitioned_data_builder_null_rate() {
        let (_, partitions) = RandomPartitionedDataBuilder::new()
            .rows_per_batch(100)
            .null_rate(0.5) // 50% null rate
            .build()
            .unwrap();

        let batch = &partitions[0][0];
        let geometry_array = batch.column(2);

        let null_count = geometry_array.null_count();
        let total_count = geometry_array.len();
        let null_rate = null_count as f64 / total_count as f64;

        // Allow some variance due to randomness (±20%)
        assert!(
            (0.3..=0.7).contains(&null_rate),
            "Expected null rate around 0.5, got {null_rate}"
        );
    }

    #[test]
    fn test_random_partitioned_data_builder_deterministic() {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });

        let (schema1, partitions1) = RandomPartitionedDataBuilder::new()
            .seed(999)
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(5)
            .bounds(bounds)
            .build()
            .unwrap();

        let (schema2, partitions2) = RandomPartitionedDataBuilder::new()
            .seed(999) // Same seed
            .num_partitions(2)
            .batches_per_partition(2)
            .rows_per_batch(5)
            .bounds(bounds)
            .build()
            .unwrap();

        // Schemas should be identical
        assert_eq!(schema1, schema2);

        // All data should be identical
        assert_eq!(partitions1.len(), partitions2.len());
        for (partition1, partition2) in partitions1.iter().zip(partitions2.iter()) {
            assert_eq!(partition1.len(), partition2.len());
            for (batch1, batch2) in partition1.iter().zip(partition2.iter()) {
                // Compare IDs
                let ids1 = batch1
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let ids2 = batch2
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                assert_eq!(ids1, ids2);

                // Compare distances
                let dists1 = batch1
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let dists2 = batch2
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                assert_eq!(dists1, dists2);
            }
        }
    }

    #[test]
    fn test_random_partitioned_data_builder_different_seeds() {
        let bounds = Rect::new(Coord { x: 0.0, y: 0.0 }, Coord { x: 100.0, y: 100.0 });

        let (_, partitions1) = RandomPartitionedDataBuilder::new()
            .seed(111)
            .rows_per_batch(10)
            .bounds(bounds)
            .build()
            .unwrap();

        let (_, partitions2) = RandomPartitionedDataBuilder::new()
            .seed(222) // Different seed
            .rows_per_batch(10)
            .bounds(bounds)
            .build()
            .unwrap();

        // Data should be different (distances should differ)
        let dists1 = partitions1[0][0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let dists2 = partitions2[0][0]
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        // At least some distances should be different
        let mut found_difference = false;
        for i in 0..dists1.len() {
            if (dists1.value(i) - dists2.value(i)).abs() > f64::EPSILON {
                found_difference = true;
                break;
            }
        }
        assert!(
            found_difference,
            "Expected different random data with different seeds"
        );
    }
}
