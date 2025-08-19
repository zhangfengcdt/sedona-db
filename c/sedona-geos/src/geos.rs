use geos::{GResult, Geom, Geometry};

#[derive(Debug, Default)]
pub struct GeosPredicate<Op> {
    _op: Op,
}

pub trait BinaryPredicate: std::fmt::Debug + Default {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool>;
}

/// Check for topological equality
#[derive(Debug, Default)]
pub struct Equals {}
impl BinaryPredicate for Equals {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.equals(rhs)
    }
}

/// Check if two geometries intersect
#[derive(Debug, Default)]
pub struct Intersects {}
impl BinaryPredicate for Intersects {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.intersects(rhs)
    }
}

/// Check if two geometries do not intersect
#[derive(Debug, Default)]
pub struct Disjoint {}
impl BinaryPredicate for Disjoint {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.disjoint(rhs)
    }
}

/// Check if the left geometry contains the right
#[derive(Debug, Default)]
pub struct Contains {}
impl BinaryPredicate for Contains {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.contains(rhs)
    }
}

/// Check if the right geometry contains the left
#[derive(Debug, Default)]
pub struct Within {}
impl BinaryPredicate for Within {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.within(rhs)
    }
}

/// Check if the left geometry covers the right geometry
#[derive(Debug, Default)]
pub struct Covers {}
impl BinaryPredicate for Covers {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.covers(rhs)
    }
}

/// Check if the right geometry covers the left geometry
#[derive(Debug, Default)]
pub struct CoveredBy {}
impl BinaryPredicate for CoveredBy {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.covered_by(rhs)
    }
}

/// Check if the geometries touch but do not otherwise intersect
#[derive(Debug, Default)]
pub struct Touches {}
impl BinaryPredicate for Touches {
    fn evaluate(lhs: &Geometry, rhs: &Geometry) -> GResult<bool> {
        lhs.touches(rhs)
    }
}
