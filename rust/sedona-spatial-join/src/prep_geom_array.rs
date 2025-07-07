use std::sync::atomic::{AtomicPtr, Ordering};

use datafusion_common::{DataFusionError, Result};
use geos::{Geom, PreparedGeometry};
use parking_lot::Mutex;

/// A wrapper around a GEOS Geometry and its corresponding PreparedGeometry.
///
/// This struct solves the self-referential lifetime problem by using unsafe transmutation
/// to extend the PreparedGeometry lifetime to 'static. This is safe because:
/// 1. The PreparedGeometry is created from self.geometry, which lives as long as self
/// 2. The PreparedGeometry is stored in self and will be dropped before self.geometry
/// 3. We only return references, never move the PreparedGeometry out
///
/// The PreparedGeometry is protected by a Mutex because it has internal mutable state
/// that is not thread-safe.
pub struct OwnedPreparedGeometry {
    geometry: geos::Geometry,
    /// PreparedGeometry references the original geometry `geometry` it is created from. The GEOS
    /// objects are allocated on the heap so moving `OwnedPreparedGeometry` does not move the
    /// underlying GEOS object, so we don't need to worry about pinning.
    ///
    /// `PreparedGeometry` is not thread-safe, because it has some lazily initialized internal states,
    /// so we need to use a `Mutex` to protect it.
    prepared_geometry: Mutex<PreparedGeometry<'static>>,
}

impl OwnedPreparedGeometry {
    /// Create a new OwnedPreparedGeometry from a GEOS Geometry.
    pub fn try_new(geometry: geos::Geometry) -> Result<Self> {
        let prepared = geometry.to_prepared_geom().map_err(|e| {
            DataFusionError::Execution(format!("Failed to create prepared geometry: {e}"))
        })?;

        // SAFETY: We're extending the lifetime of PreparedGeometry to 'static.
        // This is safe because:
        // 1. The PreparedGeometry is created from self.geometry, which lives as long as self
        // 2. The PreparedGeometry is stored in self.prepared_geometry, which also lives as long as self
        // 3. We only return references to the PreparedGeometry, never move it out
        // 4. The PreparedGeometry will be dropped when self is dropped, before self.geometry
        let prepared_static: PreparedGeometry<'static> = unsafe { std::mem::transmute(prepared) };

        Ok(Self {
            geometry,
            prepared_geometry: Mutex::new(prepared_static),
        })
    }

    /// Create a new OwnedPreparedGeometry from WKB bytes.
    pub fn try_from_wkb(wkb: &[u8]) -> Result<Self> {
        let geometry = geos::Geometry::new_from_wkb(wkb).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create geometry from WKB: {e}"))
        })?;
        Self::try_new(geometry)
    }

    /// Get access to the prepared geometry via a Mutex.
    ///
    /// The returned reference has a lifetime tied to &self, which ensures memory safety.
    /// The 'static lifetime on PreparedGeometry indicates it doesn't borrow from external data.
    pub fn prepared(&self) -> &Mutex<PreparedGeometry<'static>> {
        &self.prepared_geometry
    }

    /// Get the original geometry (for testing purposes).
    pub fn geometry(&self) -> &geos::Geometry {
        &self.geometry
    }
}

/// A thread-safe array of prepared geometries with lock-free concurrent access.
///
/// This array allows multiple threads to concurrently:
/// - Read existing prepared geometries
/// - Set new prepared geometries if slots are empty
/// - Create and cache prepared geometries on-demand
///
/// Uses atomic pointers for lock-free operations with proper memory ordering.
/// Null pointers indicate empty slots.
pub(crate) struct PreparedGeometryArray {
    geometries: Vec<AtomicPtr<OwnedPreparedGeometry>>,
}

impl PreparedGeometryArray {
    /// Create a new array with the specified size.
    /// All slots start as empty (null pointers).
    pub fn new(size: usize) -> Self {
        let geometries = (0..size)
            .map(|_| AtomicPtr::new(std::ptr::null_mut()))
            .collect();
        Self { geometries }
    }

    /// Atomically set a geometry at the given index if the slot is currently empty.
    ///
    /// Returns a reference to the geometry that is now at that index:
    /// - If the slot was empty, returns the newly set geometry
    /// - If the slot was occupied, returns the existing geometry and drops the new one
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is caller's responsibility).
    pub fn set_if_null(
        &self,
        index: usize,
        geometry: Box<OwnedPreparedGeometry>,
    ) -> &OwnedPreparedGeometry {
        let new_ptr = Box::into_raw(geometry);
        let result = self.geometries[index].compare_exchange_weak(
            std::ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        match result {
            Ok(_) => unsafe { &*new_ptr }, // Successfully set - return reference to the new geometry
            Err(old_ptr) => {
                // Drop the new geometry since it's not set. The slot at index is already occupied.
                let _ = unsafe { Box::from_raw(new_ptr) };
                unsafe { &*old_ptr }
            }
        }
    }

    /// Get the geometry at the given index, if present.
    ///
    /// Returns None if the slot is empty or index is out of bounds.
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is caller's responsibility).
    pub fn get(&self, index: usize) -> Option<&OwnedPreparedGeometry> {
        let ptr = self.geometries[index].load(Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            Some(unsafe { &*ptr })
        }
    }

    /// Get the geometry at the given index, or create and set it if not present.
    ///
    /// This is the primary method for lazy initialization of prepared geometries.
    /// Multiple threads can safely call this method concurrently.
    ///
    /// # Panics
    /// Panics if index is out of bounds (internal API, bounds checking is caller's responsibility).
    pub fn get_or_create(
        &self,
        index: usize,
        f: impl FnOnce() -> Result<OwnedPreparedGeometry>,
    ) -> Result<&OwnedPreparedGeometry> {
        if let Some(prep_geom) = self.get(index) {
            return Ok(prep_geom);
        }

        let geometry = f()?;
        Ok(self.set_if_null(index, Box::new(geometry)))
    }

    /// Get the size of the array.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.geometries.len()
    }

    /// Check if the array is empty.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.geometries.is_empty()
    }
}

impl Drop for PreparedGeometryArray {
    fn drop(&mut self) {
        for geometry in &self.geometries {
            let ptr = geometry.load(Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use sedona_testing::create::make_wkb;

    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_owned_prepared_geometry_creation() {
        let wkb = make_wkb("POINT(1.0 2.0)");
        let owned_geom = OwnedPreparedGeometry::try_from_wkb(&wkb).unwrap();

        // Test that we can access the prepared geometry
        let mutex = owned_geom.prepared();
        let guard = mutex.lock();
        // If we got here without panic, the prepared geometry was created successfully
        drop(guard);
    }

    #[test]
    fn test_owned_prepared_geometry_from_invalid_wkb() {
        let invalid_wkb = vec![0xFF, 0xFF, 0xFF]; // Invalid WKB
        let result = OwnedPreparedGeometry::try_from_wkb(&invalid_wkb);
        assert!(result.is_err());
    }

    #[test]
    fn test_prepared_geometry_array_new() {
        let array = PreparedGeometryArray::new(10);
        assert_eq!(array.len(), 10);
        assert!(!array.is_empty());

        // All slots should be empty initially
        for i in 0..10 {
            assert!(array.get(i).is_none());
        }
    }

    #[test]
    fn test_prepared_geometry_array_empty() {
        let array = PreparedGeometryArray::new(0);
        assert_eq!(array.len(), 0);
        assert!(array.is_empty());
    }

    #[test]
    fn test_set_if_null_empty_slot() {
        let array = PreparedGeometryArray::new(5);
        let wkb = make_wkb("POINT(1.0 2.0)");
        let owned_geom = Box::new(OwnedPreparedGeometry::try_from_wkb(&wkb).unwrap());

        let result = array.set_if_null(2, owned_geom);

        // Should return reference to the set geometry
        let mutex = result.prepared();
        let _guard = mutex.lock(); // Should not panic

        // Slot should now be occupied
        assert!(array.get(2).is_some());
    }

    #[test]
    fn test_set_if_null_occupied_slot() {
        let array = PreparedGeometryArray::new(5);
        let wkb1 = make_wkb("POINT(1.0 2.0)");
        let wkb2 = make_wkb("POINT(3.0 4.0)");

        let owned_geom1 = Box::new(OwnedPreparedGeometry::try_from_wkb(&wkb1).unwrap());
        let owned_geom2 = Box::new(OwnedPreparedGeometry::try_from_wkb(&wkb2).unwrap());

        // Set first geometry
        let result1 = array.set_if_null(2, owned_geom1);

        // Try to set second geometry in same slot
        let result2 = array.set_if_null(2, owned_geom2);

        // Both should return the same geometry (the first one)
        let ptr1 = result1 as *const OwnedPreparedGeometry;
        let ptr2 = result2 as *const OwnedPreparedGeometry;
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn test_get_or_create() {
        let array = PreparedGeometryArray::new(5);
        let wkb = make_wkb("POINT(1.0 2.0)");

        // First call should create the geometry
        let result1 = array.get_or_create(2, || OwnedPreparedGeometry::try_from_wkb(&wkb));

        // Second call should return the same geometry
        let result2 = array.get_or_create(2, || {
            panic!("Should not be called - geometry already exists")
        });

        let ptr1 = result1.unwrap() as *const OwnedPreparedGeometry;
        let ptr2 = result2.unwrap() as *const OwnedPreparedGeometry;
        assert_eq!(ptr1, ptr2);
    }

    #[test]
    fn test_get_or_create_error() {
        let array = PreparedGeometryArray::new(5);
        let result = array.get_or_create(2, || {
            Err(DataFusionError::Execution(
                "Failed to create geometry".to_string(),
            ))
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_concurrent_set_if_null() {
        let array = Arc::new(PreparedGeometryArray::new(100));
        let mut handles = vec![];

        // Spawn multiple threads trying to set the same slot
        for i in 0..10 {
            let array_clone = Arc::clone(&array);
            let handle = thread::spawn(move || {
                let wkb = make_wkb(&format!("POINT({i}.0 {i}.0)"));
                let owned_geom = Box::new(OwnedPreparedGeometry::try_from_wkb(&wkb).unwrap());
                let result = array_clone.set_if_null(50, owned_geom);
                result as *const OwnedPreparedGeometry as usize // Return pointer address as usize
            });
            handles.push(handle);
        }

        // Collect all results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.join().unwrap());
        }

        // All threads should return the same geometry reference
        let first_ptr = results[0];
        for result in &results[1..] {
            assert_eq!(*result, first_ptr);
        }

        // Only one geometry should be in the array
        assert!(array.get(50).is_some());
    }

    #[test]
    fn test_concurrent_get_or_create() {
        let array = Arc::new(PreparedGeometryArray::new(100));
        let mut handles = vec![];

        // Spawn multiple threads trying to get_or_create the same slot
        for i in 0..10 {
            let array_clone = Arc::clone(&array);
            let handle = thread::spawn(move || {
                let result = array_clone.get_or_create(75, || {
                    let wkb = make_wkb(&format!("POINT({i}.0 {i}.0)"));
                    OwnedPreparedGeometry::try_from_wkb(&wkb)
                });
                result.unwrap() as *const OwnedPreparedGeometry as usize // Return pointer address as usize
            });
            handles.push(handle);
        }

        // Collect all results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.join().unwrap());
        }

        // All threads should return the same geometry reference
        let first_ptr = results[0];
        for result in &results[1..] {
            assert_eq!(*result, first_ptr);
        }
    }

    #[test]
    fn test_concurrent_different_indices() {
        let array = Arc::new(PreparedGeometryArray::new(100));
        let mut handles = vec![];

        // Spawn threads targeting different indices
        for i in 0..10 {
            let array_clone = Arc::clone(&array);
            let handle = thread::spawn(move || {
                array_clone
                    .get_or_create(i, || {
                        let wkb = make_wkb(&format!("POINT({i}.0 {i}.0)"));
                        OwnedPreparedGeometry::try_from_wkb(&wkb)
                    })
                    .unwrap_or_else(|_| panic!("Failed to create geometry for index {i}"));
                // Just return () since we don't need the reference
            });
            handles.push(handle);
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // All indices should be filled
        for i in 0..10 {
            assert!(array.get(i).is_some());
        }

        // Indices beyond 10 should be empty
        for i in 10..20 {
            assert!(array.get(i).is_none());
        }
    }

    #[test]
    fn test_drop_cleans_up_memory() {
        // This test ensures that Drop implementation properly cleans up
        {
            let array = PreparedGeometryArray::new(5);
            for i in 0..5 {
                let wkb = make_wkb(&format!("POINT({i}.0 {i}.0)"));
                let owned_geom = Box::new(OwnedPreparedGeometry::try_from_wkb(&wkb).unwrap());
                array.set_if_null(i, owned_geom);
            }
            // array goes out of scope here and should clean up all geometries
        }
        // If we reach here without crashing, the drop worked correctly
    }

    #[test]
    fn test_send_sync_traits() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}

        assert_send::<PreparedGeometryArray>();
        assert_sync::<PreparedGeometryArray>();
    }

    #[test]
    fn test_prepared_geometry_usage() {
        let array = PreparedGeometryArray::new(5);
        let wkb1 = make_wkb("POINT(1.0 2.0)");
        let wkb2 = make_wkb("POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))");

        // Set geometries
        let point_geom = array
            .get_or_create(0, || OwnedPreparedGeometry::try_from_wkb(&wkb1))
            .expect("Failed to create point geometry");
        let poly_geom = array
            .get_or_create(1, || OwnedPreparedGeometry::try_from_wkb(&wkb2))
            .expect("Failed to create polygon geometry");

        // Use the prepared geometries for spatial operations
        let point_prep = point_geom.prepared();
        let poly_prep = poly_geom.prepared();

        let point_guard = point_prep.lock();
        let poly_guard = poly_prep.lock();

        assert!(point_guard
            .intersects(poly_geom.geometry())
            .expect("Failed to intersects"));
        assert!(poly_guard
            .contains(point_geom.geometry())
            .expect("Failed to contains"));
    }
}
