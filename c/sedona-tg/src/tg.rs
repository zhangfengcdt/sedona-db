use std::fmt::Display;

use crate::{error::TgError, tg_bindgen::*};

/// Index types available from tg
///
/// These index types are used to inform the internal index used in geometries
/// to accelerate repeated operations.
#[derive(Debug, Clone, Copy)]
pub enum IndexType {
    /// Do not build an index
    Unindexed,
    /// Use the statically set default
    Default,
    /// Use natural indexing
    Natural,
    /// Use y-stripes indexing
    YStripes,
}

impl Default for IndexType {
    fn default() -> Self {
        Self::Default
    }
}

impl IndexType {
    fn to_tg(self) -> u32 {
        match self {
            IndexType::Unindexed => tg_index_TG_NONE,
            IndexType::Default => tg_index_TG_DEFAULT,
            IndexType::Natural => tg_index_TG_NATURAL,
            IndexType::YStripes => tg_index_TG_YSTRIPES,
        }
    }
}

/// tg Geometry Class
///
/// The Geom is a generic geometry variant like a GeometryTrait, GEOSGeometry,
/// or Wkb. This class wraps a pinned, heap-allocated tg_geom.
pub struct Geom {
    inner: *mut tg_geom,
}

impl Drop for Geom {
    fn drop(&mut self) {
        if self.inner.is_null() {
            return;
        }

        unsafe { tg_geom_free(self.inner) };
    }
}

impl Geom {
    /// Create a [Geom] from well-known text
    pub fn parse_wkt(wkt: &str, index: IndexType) -> Result<Self, TgError> {
        unsafe {
            let inner = tg_parse_wktn_ix(wkt.as_ptr() as *const i8, wkt.len(), index.to_tg());
            Self::try_new(inner)
        }
    }

    /// Create a [Geom] from well-known binary
    pub fn parse_wkb(wkb: &[u8], index: IndexType) -> Result<Self, TgError> {
        unsafe {
            let inner = tg_parse_wkb_ix(wkb.as_ptr(), wkb.len(), index.to_tg());
            Self::try_new(inner)
        }
    }

    unsafe fn try_new(inner: *mut tg_geom) -> Result<Self, TgError> {
        if inner.is_null() {
            return Err(TgError::Memory);
        }

        let geom = Self { inner };
        let err = tg_geom_error(geom.inner);
        if err.is_null() {
            return Ok(geom);
        }

        let c_str = std::ffi::CStr::from_ptr(err);
        Err(TgError::Invalid(c_str.to_string_lossy().into_owned()))
    }

    /// Write this [Geom] as well-known binary into out
    ///
    /// Callers must check the return size to ensure that all bytes were written.
    /// Use [Self::to_wkb] for a slower but more convenient way to obtain the result.
    pub fn write_wkb(&self, out: &mut [u8]) -> usize {
        unsafe { tg_geom_wkb(self.inner, out.as_mut_ptr(), out.len()) }
    }

    /// Write this [Geom] as well-known text into out
    ///
    /// Callers must check the return size to ensure that all bytes were written.
    /// Use [Self::to_wkt] for a slower but more convenient way to obtain the result.
    pub fn write_wkt(&self, out: &mut [u8]) -> usize {
        unsafe { tg_geom_wkt(self.inner, out.as_mut_ptr() as *mut i8, out.len()) }
    }

    /// Write this [Geom] to well-known binary as a [Vec]
    #[allow(clippy::uninit_vec)]
    pub fn to_wkb(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let size_required = self.write_wkb(&mut []);
        out.reserve(size_required);
        unsafe {
            out.set_len(size_required);
            self.write_wkb(&mut out);
        }
        out
    }

    /// Write this [Geom] to well-known binary as a [String]
    pub fn to_wkt(&self) -> String {
        let mut out = String::new();
        let size_required = self.write_wkt(&mut []);
        out.reserve(size_required + 1);
        unsafe {
            out.as_mut_vec().set_len(size_required + 1);
            self.write_wkt(out.as_mut_vec());
            out.as_mut_vec().set_len(size_required);
        }
        out
    }

    pub fn memsize(&self) -> usize {
        unsafe { tg_geom_memsize(self.inner) }
    }
}

impl Display for Geom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_wkt())
    }
}

/// Trait to make operations generic over binary predicates
pub trait BinaryPredicate: std::fmt::Debug + Default {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool;
}

/// Check for topologicall equal geometries
#[derive(Debug, Default)]
pub struct Equals {}
impl BinaryPredicate for Equals {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_equals(lhs.inner, rhs.inner) }
    }
}

/// Check if two geometries intersect
#[derive(Debug, Default)]
pub struct Intersects {}
impl BinaryPredicate for Intersects {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_intersects(lhs.inner, rhs.inner) }
    }
}

/// Check if two geometries do not intersect
#[derive(Debug, Default)]
pub struct Disjoint {}
impl BinaryPredicate for Disjoint {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_disjoint(lhs.inner, rhs.inner) }
    }
}

/// Check if the left geometry contains the right
#[derive(Debug, Default)]
pub struct Contains {}
impl BinaryPredicate for Contains {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_contains(lhs.inner, rhs.inner) }
    }
}

/// Check if the right geometry contains the left
#[derive(Debug, Default)]
pub struct Within {}
impl BinaryPredicate for Within {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_within(lhs.inner, rhs.inner) }
    }
}

/// Check if the left geometry covers the right geometry
#[derive(Debug, Default)]
pub struct Covers {}
impl BinaryPredicate for Covers {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_covers(lhs.inner, rhs.inner) }
    }
}

/// Check if the right geometry covers the left geometry
#[derive(Debug, Default)]
pub struct CoveredBy {}
impl BinaryPredicate for CoveredBy {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_coveredby(lhs.inner, rhs.inner) }
    }
}

/// Check if the geometries touch but do not otherwise intersect
#[derive(Debug, Default)]
pub struct Touches {}
impl BinaryPredicate for Touches {
    fn evaluate(lhs: &Geom, rhs: &Geom) -> bool {
        unsafe { tg_geom_touches(lhs.inner, rhs.inner) }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn geom() {
        let geom = Geom::parse_wkt("POINT (0 1)", Default::default()).unwrap();
        assert_eq!(geom.to_wkt(), "POINT(0 1)");

        let wkb = geom.to_wkb();
        assert!(!wkb.is_empty());
        let geom2 = Geom::parse_wkb(&wkb, Default::default()).unwrap();
        assert_eq!(geom2.to_wkt(), "POINT(0 1)");

        assert!(geom.memsize() > 0);
    }

    fn test_predicate<Op: BinaryPredicate>(lhs: &str, rhs: &str, expected: bool) {
        let lhs_geom = Geom::parse_wkt(lhs, Default::default()).unwrap();
        let rhs_geom = Geom::parse_wkt(rhs, Default::default()).unwrap();
        if Op::evaluate(&lhs_geom, &rhs_geom) != expected {
            panic!(
                "Expected {lhs_geom} {:?} {rhs_geom} == {expected}",
                Op::default()
            )
        }
    }

    #[test]
    fn predicates() {
        test_predicate::<Equals>("POINT (0 1)", "POINT (0 1)", true);
        test_predicate::<Equals>("POINT (0 1)", "POINT (0 0)", false);

        test_predicate::<Intersects>("POINT (0 1)", "POINT (0 1)", true);
        test_predicate::<Intersects>("POINT (0 1)", "POINT (0 0)", false);

        test_predicate::<Disjoint>("POINT (0 1)", "POINT (0 1)", false);
        test_predicate::<Disjoint>("POINT (0 1)", "POINT (0 0)", true);

        test_predicate::<Contains>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (1 1)", true);
        test_predicate::<Contains>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (-1 -1)", false);

        test_predicate::<Within>("POINT (1 1)", "POLYGON ((0 0, 5 0, 0 5, 0 0))", true);
        test_predicate::<Within>("POINT (-1 -1)", "POLYGON ((0 0, 5 0, 0 5, 0 0))", false);

        test_predicate::<Covers>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (1 1)", true);
        test_predicate::<Covers>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (-1 -1)", false);

        test_predicate::<CoveredBy>("POINT (1 1)", "POLYGON ((0 0, 5 0, 0 5, 0 0))", true);
        test_predicate::<CoveredBy>("POINT (-1 -1)", "POLYGON ((0 0, 5 0, 0 5, 0 0))", false);

        test_predicate::<Touches>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (0 0)", true);
        test_predicate::<Touches>("POLYGON ((0 0, 5 0, 0 5, 0 0))", "POINT (1 1)", false);
    }
}
