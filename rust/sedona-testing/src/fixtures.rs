// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::{fs::File, path::PathBuf, str::FromStr};

use geo_types::{LineString, MultiPolygon, Point, Polygon};
use wkt::{TryFromWkt, WktFloat};

/// A well-known binary blob of MULTIPOINT (EMPTY)
///
/// The wkt crate's parser rejects this; however, it's a corner case that may show
/// up in WKB generated externally.
pub const MULTIPOINT_WITH_EMPTY_CHILD_WKB: [u8; 30] = [
    0x01, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x7f,
];

pub fn louisiana<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("louisiana.wkt")
}

pub fn baton_rouge<T>() -> Point<T>
where
    T: WktFloat + Default + FromStr,
{
    let x = T::from(-91.147385).unwrap();
    let y = T::from(30.471165).unwrap();
    Point::new(x, y)
}

pub fn east_baton_rouge<T>() -> Polygon<T>
where
    T: WktFloat + Default + FromStr,
{
    polygon("east_baton_rouge.wkt")
}

pub fn norway_main<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("norway_main.wkt")
}

pub fn norway_concave_hull<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("norway_concave_hull.wkt")
}

pub fn norway_convex_hull<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("norway_convex_hull.wkt")
}

pub fn norway_nonconvex_hull<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("norway_nonconvex_hull.wkt")
}

pub fn vw_orig<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("vw_orig.wkt")
}

pub fn vw_simplified<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("vw_simplified.wkt")
}

pub fn poly1<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("poly1.wkt")
}

pub fn poly1_hull<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("poly1_hull.wkt")
}

pub fn poly2<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("poly2.wkt")
}

pub fn poly2_hull<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("poly2_hull.wkt")
}

pub fn poly_in_ring<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("poly_in_ring.wkt")
}

pub fn ring<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("ring.wkt")
}

pub fn shell<T>() -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    line_string("shell.wkt")
}

// From https://geodata.nationaalgeoregister.nl/kadastralekaart/wfs/v4_0?request=GetFeature&service=WFS&srsName=EPSG:4326&typeName=kadastralekaartv4:perceel&version=2.0.0&outputFormat=json&bbox=165593,480993,166125,481552
pub fn nl_zones<T>() -> MultiPolygon<T>
where
    T: WktFloat + Default + FromStr,
{
    multi_polygon("nl_zones.wkt")
}

// From https://afnemers.ruimtelijkeplannen.nl/afnemers/services?request=GetFeature&service=WFS&srsName=EPSG:4326&typeName=Enkelbestemming&version=2.0.0&bbox=165618,480983,166149,481542";
pub fn nl_plots_wgs84<T>() -> MultiPolygon<T>
where
    T: WktFloat + Default + FromStr,
{
    multi_polygon("nl_plots.wkt")
}

pub fn nl_plots_epsg_28992<T>() -> MultiPolygon<T>
where
    T: WktFloat + Default + FromStr,
{
    // https://epsg.io/28992
    multi_polygon("nl_plots_epsg_28992.wkt")
}

fn line_string<T>(name: &str) -> LineString<T>
where
    T: WktFloat + Default + FromStr,
{
    LineString::try_from_wkt_reader(file(name)).unwrap()
}

pub fn polygon<T>(name: &str) -> Polygon<T>
where
    T: WktFloat + Default + FromStr,
{
    Polygon::try_from_wkt_reader(file(name)).unwrap()
}

pub fn multi_polygon<T>(name: &str) -> MultiPolygon<T>
where
    T: WktFloat + Default + FromStr,
{
    MultiPolygon::try_from_wkt_reader(file(name)).unwrap()
}

pub fn file(name: &str) -> File {
    let base = crate::data::sedona_testing_dir()
        .expect("sedona-testing directory should resolve when accessing fixtures");

    let mut path = PathBuf::from(base);
    path.push("data");
    path.push("wkts");
    path.push("geo-test-fixtures");
    path.push(name);

    File::open(&path).unwrap_or_else(|_| panic!("Can't open file: {path:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn norway_main_linestring_has_vertices() {
        let ls = norway_main::<f64>();
        assert!(
            !ls.0.is_empty(),
            "LineString loaded from norway_main.wkt should have vertices"
        );

        let first = ls.0.first().expect("expected at least one coordinate");
        assert!(first.x.is_finite(), "first coordinate x should be finite");
        assert!(first.y.is_finite(), "first coordinate y should be finite");
    }

    #[test]
    fn nl_zones_multipolygon_not_empty() {
        let mp = nl_zones::<f64>();
        assert!(
            !mp.0.is_empty(),
            "MultiPolygon from nl_zones.wkt should contain polygons"
        );

        let polygon = mp.0.first().expect("expected at least one polygon");
        assert!(
            !polygon.exterior().0.is_empty(),
            "polygon exterior ring should contain coordinates"
        );
    }
}
