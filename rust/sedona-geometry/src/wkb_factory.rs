use std::io::Write;

use crate::error::SedonaGeometryError;

/// Create WKB representing a POINT
///
/// A convenience wrapper for [write_wkb_point] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_point(pt: (f64, f64)) -> Result<Vec<u8>, SedonaGeometryError> {
    let mut out_wkb = Vec::with_capacity(21);
    write_wkb_point(&mut out_wkb, pt)?;
    Ok(out_wkb)
}

/// Write WKB representing a POINT into a buffer
///
/// This can be used to build Binary arrays, as the arrow-rs BinaryBuilder
/// implements Write.
pub fn write_wkb_point(buf: &mut impl Write, pt: (f64, f64)) -> Result<(), SedonaGeometryError> {
    buf.write_all(&[0x01, 0x01, 0x00, 0x00, 0x00])?;
    buf.write_all(&pt.0.to_le_bytes())?;
    buf.write_all(&pt.1.to_le_bytes())?;
    Ok(())
}

/// Create WKB representing a LINESTRING
///
/// A convenience wrapper for [write_wkb_linestring] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_linestring<I: ExactSizeIterator<Item = (f64, f64)>>(
    pts: I,
) -> Result<Vec<u8>, SedonaGeometryError> {
    let mut out_wkb = Vec::with_capacity(5 + 4 + pts.len() * 16);
    write_wkb_linestring(&mut out_wkb, pts)?;
    Ok(out_wkb)
}

/// Write WKB representing a LINESTRING into a buffer
///
/// This can be used to build Binary arrays, as the arrow-rs BinaryBuilder
/// implements Write.
pub fn write_wkb_linestring<I: ExactSizeIterator<Item = (f64, f64)>>(
    buf: &mut impl Write,
    pts: I,
) -> Result<(), SedonaGeometryError> {
    let size_u32: u32 = pts.len().try_into()?;

    buf.write_all(&[0x01, 0x02, 0x00, 0x00, 0x00])?;
    buf.write_all(&size_u32.to_le_bytes())?;
    for pt in pts {
        buf.write_all(&pt.0.to_le_bytes())?;
        buf.write_all(&pt.1.to_le_bytes())?;
    }

    Ok(())
}

/// Create WKB representing a MULTILINESTRING
///
/// A convenience wrapper for [write_wkb_multilinestring] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_multilinestring<I>(linestrings: I) -> Result<Vec<u8>, SedonaGeometryError>
where
    I: ExactSizeIterator<Item = Vec<(f64, f64)>> + Clone,
{
    // Base size + linestrings count + estimated per-linestring overhead
    let total_points: usize = linestrings.clone().map(|ls| ls.len()).sum();
    let capacity = 5 + 4 + (linestrings.len() * 9) + (total_points * 16);
    let mut out_wkb = Vec::with_capacity(capacity);
    write_wkb_multilinestring(&mut out_wkb, linestrings)?;
    Ok(out_wkb)
}

/// Write WKB representing a MULTILINESTRING into a buffer
///
/// This can be used to build Binary arrays, as the arrow-rs BinaryBuilder
/// implements Write.
pub fn write_wkb_multilinestring<I>(
    buf: &mut impl Write,
    linestrings: I,
) -> Result<(), SedonaGeometryError>
where
    I: ExactSizeIterator<Item = Vec<(f64, f64)>>,
{
    let num_linestrings: u32 = linestrings.len().try_into()?;

    // Write header: byte order (little endian) and geometry type (5 for MultiLineString)
    buf.write_all(&[0x01, 0x05, 0x00, 0x00, 0x00])?;

    // Write number of linestrings
    buf.write_all(&num_linestrings.to_le_bytes())?;

    // For each linestring, write a complete linestring WKB
    for linestring in linestrings {
        // Each linestring needs its own byte order and type
        buf.write_all(&[0x01, 0x02, 0x00, 0x00, 0x00])?;

        // Write number of points in the linestring
        let num_points: u32 = linestring.len().try_into()?;
        buf.write_all(&num_points.to_le_bytes())?;

        // Write each point coordinate
        for pt in linestring {
            buf.write_all(&pt.0.to_le_bytes())?;
            buf.write_all(&pt.1.to_le_bytes())?;
        }
    }

    Ok(())
}

/// Create WKB representing a POLYGON
///
/// A convenience wrapper for [write_wkb_polygon] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_polygon<I: ExactSizeIterator<Item = (f64, f64)>>(
    pts: I,
) -> Result<Vec<u8>, SedonaGeometryError> {
    let mut out_wkb = Vec::with_capacity(5 + 4 + 4 + pts.len() * 16);
    write_wkb_polygon(&mut out_wkb, pts)?;
    Ok(out_wkb)
}

/// Write WKB representing a POLYGON into a buffer
///
/// This can be used to build Binary arrays, as the arrow-rs BinaryBuilder
/// implements Write.
pub fn write_wkb_polygon<I: ExactSizeIterator<Item = (f64, f64)>>(
    buf: &mut impl Write,
    pts: I,
) -> Result<(), SedonaGeometryError> {
    let size_u32: u32 = pts.len().try_into()?;

    buf.write_all(&[0x01, 0x03, 0x00, 0x00, 0x00])?;

    // For zero points, write POLYGON EMPTY
    if size_u32 == 0 {
        buf.write_all(&[0x00, 0x00, 0x00, 0x00])?;
        return Ok(());
    }

    // For >= 0 points, write a single ring with n points
    buf.write_all(&[0x01, 0x00, 0x00, 0x00])?;
    buf.write_all(&size_u32.to_le_bytes())?;
    for pt in pts {
        buf.write_all(&pt.0.to_le_bytes())?;
        buf.write_all(&pt.1.to_le_bytes())?;
    }

    Ok(())
}

/// Create WKB representing a MULTIPOLYGON
///
/// A convenience wrapper for [write_wkb_multipolygon] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_multipolygon<I>(polygons: I) -> Result<Vec<u8>, SedonaGeometryError>
where
    I: ExactSizeIterator<Item = Vec<(f64, f64)>>,
{
    // Base size + polygons count + memory for coordinate pairs and ring headers
    let capacity = 5 + 4 + polygons.len() * (4 + 16 * polygons.len());
    let mut out_wkb = Vec::with_capacity(capacity);
    write_wkb_multipolygon(&mut out_wkb, polygons)?;
    Ok(out_wkb)
}

/// Write WKB representing a MULTIPOLYGON into a buffer
///
/// This can be used to build Binary arrays, as the arrow-rs BinaryBuilder
/// implements Write.
pub fn write_wkb_multipolygon<I>(
    buf: &mut impl Write,
    polygons: I,
) -> Result<(), SedonaGeometryError>
where
    I: ExactSizeIterator<Item = Vec<(f64, f64)>>,
{
    let num_polygons: u32 = polygons.len().try_into()?;

    // Write header: byte order (little endian) and geometry type (6 for MultiPolygon)
    buf.write_all(&[0x01, 0x06, 0x00, 0x00, 0x00])?;

    // Write number of polygons
    buf.write_all(&num_polygons.to_le_bytes())?;

    // For each polygon, write a complete polygon WKB
    for polygon in polygons {
        // Each polygon needs its own byte order and type
        buf.write_all(&[0x01, 0x03, 0x00, 0x00, 0x00])?;

        // If polygon is empty
        if polygon.is_empty() {
            buf.write_all(&[0x00, 0x00, 0x00, 0x00])?; // 0 rings
            continue;
        }

        // Write 1 ring (exterior only for now)
        buf.write_all(&[0x01, 0x00, 0x00, 0x00])?;

        // Write number of points in the polygon
        let num_points: u32 = polygon.len().try_into()?;
        buf.write_all(&num_points.to_le_bytes())?;

        // Write each point coordinate
        for pt in polygon {
            buf.write_all(&pt.0.to_le_bytes())?;
            buf.write_all(&pt.1.to_le_bytes())?;
        }
    }

    Ok(())
}

/// Create WKB representing a MULTIPOINT
///
/// A convenience wrapper for [write_wkb_multipoint] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn wkb_multipoint<I>(points: I) -> Result<Vec<u8>, SedonaGeometryError>
where
    I: ExactSizeIterator<Item = (f64, f64)>,
{
    // Base size + points count
    let capacity = 5 + 4 + points.len() * 16;
    let mut out_wkb = Vec::with_capacity(capacity);
    write_wkb_multipoint(&mut out_wkb, points)?;
    Ok(out_wkb)
}

/// Write WKB representing a MULTIPOINT into a buffer
///
/// A convenience wrapper for [write_wkb_multipoint] that creates a Vec,
/// which is useful for creating DataFusion scalar values.
pub fn write_wkb_multipoint<I>(buf: &mut impl Write, points: I) -> Result<(), SedonaGeometryError>
where
    I: ExactSizeIterator<Item = (f64, f64)>,
{
    let num_points: u32 = points.len().try_into()?;

    // Write header: byte order (little endian) and geometry type (4 for MultiPoint)
    buf.write_all(&[0x01, 0x04, 0x00, 0x00, 0x00])?;

    // Write number of points
    buf.write_all(&num_points.to_le_bytes())?;

    // For each point, write a complete point WKB
    for point in points {
        // Each point needs its own byte order and type
        buf.write_all(&[0x01, 0x01, 0x00, 0x00, 0x00])?;

        buf.write_all(&point.0.to_le_bytes())?;
        buf.write_all(&point.1.to_le_bytes())?;
    }

    Ok(())
}

/// Write WKB header for GEOMETRYCOLLECTION
pub fn write_wkb_geometrycollection_header(
    buf: &mut impl Write,
    count: usize,
) -> Result<(), SedonaGeometryError> {
    buf.write_all(&[0x01, 0x07, 0x00, 0x00, 0x00])?;

    buf.write_all(&count_to_u32(count)?.to_le_bytes())?;
    Ok(())
}

/// Write WKB header for MULTIPOLYGON
pub fn write_wkb_multipolygon_header(
    buf: &mut impl Write,
    count: usize,
) -> Result<(), SedonaGeometryError> {
    buf.write_all(&[0x01, 0x06, 0x00, 0x00, 0x00])?;
    buf.write_all(&count_to_u32(count)?.to_le_bytes())?;
    Ok(())
}

/// Write WKB header for MULTIPOINT
pub fn write_wkb_multipoint_header(
    buf: &mut impl Write,
    count: usize,
) -> Result<(), SedonaGeometryError> {
    buf.write_all(&[0x01, 0x04, 0x00, 0x00, 0x00])?;
    buf.write_all(&count_to_u32(count)?.to_le_bytes())?;
    Ok(())
}

/// Write WKB header for MULTILINESTRING
pub fn write_wkb_multilinestring_header(
    buf: &mut impl Write,
    count: usize,
) -> Result<(), SedonaGeometryError> {
    buf.write_all(&[0x01, 0x05, 0x00, 0x00, 0x00])?;
    buf.write_all(&count_to_u32(count)?.to_le_bytes())?;
    Ok(())
}

fn count_to_u32(count: usize) -> Result<u32, SedonaGeometryError> {
    count.try_into().map_err(|_| {
        SedonaGeometryError::Invalid(
            "Collection contains too many elements for WKB format".to_string(),
        )
    })
}

#[cfg(test)]
mod test {

    use std::str::FromStr;
    use wkb::writer::write_geometry;
    use wkt::Wkt;

    use super::*;

    #[test]
    fn test_wkb_point() {
        let wkt: Wkt = Wkt::from_str("POINT (0 1)").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_point((0.0, 1.0)).unwrap(), wkb);
    }

    #[test]
    fn test_wkb_linestring() {
        let wkt: Wkt = Wkt::from_str("LINESTRING EMPTY").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_linestring([].into_iter()).unwrap(), wkb);

        let wkt: Wkt = Wkt::from_str("LINESTRING (0 1, 2 3)").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(
            wkb_linestring([(0.0, 1.0), (2.0, 3.0)].into_iter()).unwrap(),
            wkb
        );
    }

    #[test]
    fn test_wkb_multilinestring() {
        let wkt: Wkt = Wkt::from_str("MULTILINESTRING EMPTY").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_multilinestring([].into_iter()).unwrap(), wkb);

        let wkt: Wkt = Wkt::from_str("MULTILINESTRING ((0 0, 1 1, 2 2), (3 3, 4 4))").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();

        let linestrings = vec![
            vec![(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)],
            vec![(3.0, 3.0), (4.0, 4.0)],
        ];

        assert_eq!(wkb_multilinestring(linestrings.into_iter()).unwrap(), wkb);
    }

    #[test]
    fn test_wkb_polygon() {
        let wkt: Wkt = Wkt::from_str("POLYGON EMPTY").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_polygon([].into_iter()).unwrap(), wkb);

        let wkt: Wkt = Wkt::from_str("POLYGON ((0 0, 1 0, 0 1, 0 0))").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(
            wkb_polygon([(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)].into_iter()).unwrap(),
            wkb
        );
    }

    #[test]
    fn test_wkb_multipolygon() {
        let wkt: Wkt = Wkt::from_str("MULTIPOLYGON EMPTY").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_multipolygon([].into_iter()).unwrap(), wkb);

        let wkt: Wkt =
            Wkt::from_str("MULTIPOLYGON (((0 0, 1 0, 0 1, 0 0)), ((2 2, 3 2, 2 3, 2 2)))").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();

        let polygons = vec![
            vec![(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (0.0, 0.0)],
            vec![(2.0, 2.0), (3.0, 2.0), (2.0, 3.0), (2.0, 2.0)],
        ];

        assert_eq!(wkb_multipolygon(polygons.into_iter()).unwrap(), wkb);
    }

    #[test]
    fn test_wkb_multipoint() {
        let wkt: Wkt = Wkt::from_str("MULTIPOINT EMPTY").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();
        assert_eq!(wkb_multipoint([].into_iter()).unwrap(), wkb);

        let wkt: Wkt = Wkt::from_str("MULTIPOINT ((0 0), (1 1))").unwrap();
        let mut wkb = vec![];
        write_geometry(&mut wkb, &wkt, wkb::Endianness::LittleEndian).unwrap();

        let points = vec![(0.0, 0.0), (1.0, 1.0)];
        assert_eq!(wkb_multipoint(points.into_iter()).unwrap(), wkb);
    }

    #[test]
    fn test_wkb_multilinestring_header() {
        // Test empty header
        let mut wkb = Vec::new();
        write_wkb_multilinestring_header(&mut wkb, 0).unwrap();

        // Expected bytes for empty multilinestring:
        // - 0x01 for little endian byte order
        // - 0x05, 0x00, 0x00, 0x00 for geometry type 5 (MultiLineString)
        // - 0x00, 0x00, 0x00, 0x00 for count of 0 linestrings
        let expected = vec![0x01, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);

        // Test non-empty header
        let mut wkb = Vec::new();
        write_wkb_multilinestring_header(&mut wkb, 3).unwrap();

        // Expected bytes for multilinestring with 3 linestrings:
        // - 0x01 for little endian byte order
        // - 0x05, 0x00, 0x00, 0x00 for geometry type 5 (MultiLineString)
        // - 0x03, 0x00, 0x00, 0x00 for count of 3 linestrings
        let expected = vec![0x01, 0x05, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);
    }

    #[test]
    fn test_wkb_multipoint_header() {
        // Test empty header
        let mut wkb = Vec::new();
        write_wkb_multipoint_header(&mut wkb, 0).unwrap();

        // Expected bytes for empty multipoint:
        // - 0x01 for little endian byte order
        // - 0x04, 0x00, 0x00, 0x00 for geometry type 4 (MultiPoint)
        // - 0x00, 0x00, 0x00, 0x00 for count of 0 points
        let expected = vec![0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);

        // Test non-empty header
        let mut wkb = Vec::new();
        write_wkb_multipoint_header(&mut wkb, 5).unwrap();

        // Expected bytes for multipoint with 5 points:
        // - 0x01 for little endian byte order
        // - 0x04, 0x00, 0x00, 0x00 for geometry type 4 (MultiPoint)
        // - 0x05, 0x00, 0x00, 0x00 for count of 5 points
        let expected = vec![0x01, 0x04, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);
    }

    #[test]
    fn test_wkb_multipolygon_header() {
        // Test empty header
        let mut wkb = Vec::new();
        write_wkb_multipolygon_header(&mut wkb, 0).unwrap();

        // Expected bytes for empty multipolygon:
        // - 0x01 for little endian byte order
        // - 0x06, 0x00, 0x00, 0x00 for geometry type 6 (MultiPolygon)
        // - 0x00, 0x00, 0x00, 0x00 for count of 0 polygons
        let expected = vec![0x01, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);

        // Test non-empty header
        let mut wkb = Vec::new();
        write_wkb_multipolygon_header(&mut wkb, 4).unwrap();

        // Expected bytes for multipolygon with 4 polygons:
        // - 0x01 for little endian byte order
        // - 0x06, 0x00, 0x00, 0x00 for geometry type 6 (MultiPolygon)
        // - 0x04, 0x00, 0x00, 0x00 for count of 4 polygons
        let expected = vec![0x01, 0x06, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);
    }

    #[test]
    fn test_wkb_geometrycollection_header() {
        let mut wkb = Vec::new();
        write_wkb_geometrycollection_header(&mut wkb, 0).unwrap();
        let expected = vec![0x01, 0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);

        let mut wkb = Vec::new();
        write_wkb_geometrycollection_header(&mut wkb, 2).unwrap();

        // Expected bytes:
        // - 0x01 for little endian byte order
        // - 0x07, 0x00, 0x00, 0x00 for geometry type 7 (GeometryCollection)
        // - 0x02, 0x00, 0x00, 0x00 for count of 2 geometries
        let expected = vec![0x01, 0x07, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00];
        assert_eq!(wkb, expected);
    }
}
