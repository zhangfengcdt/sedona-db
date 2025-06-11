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
}
