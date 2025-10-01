#include "wkb.hpp"

namespace ngen {
namespace geopackage {

enum wkb_geom_t {
    geometry            = 0,
    point               = 1,
    linestring          = 2,
    polygon             = 3,
    multipoint          = 4,
    multilinestring     = 5,
    multipolygon        = 6,
    geometry_collection = 7
};

void throw_if_not_type(uint32_t given, wkb_geom_t expected)
{
    if (given != expected) {
        throw std::runtime_error(
            "expected WKB geometry type " +
            std::to_string(expected) +
            ", but received " +
            std::to_string(given)
        );
    }
}

// ----------------------------------------------------------------------------
// WKB Readers
// ----------------------------------------------------------------------------

typename wkb::point_t wkb::read_point(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    double x, y;
    utils::copy_from(buffer, index, x, order);
    utils::copy_from(buffer, index, y, order);
    return point_t{x, y};
}

// ----------------------------------------------------------------------------

typename wkb::linestring_t wkb::read_linestring(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    uint32_t count;
    utils::copy_from(buffer, index, count, order);

    linestring_t linestring;
    linestring.resize(count);
    for (auto& child : linestring) {
        child = read_point(buffer, index, order);
    }

    return linestring;
}

// ----------------------------------------------------------------------------

typename wkb::polygon_t wkb::read_polygon(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    uint32_t count;
    utils::copy_from(buffer, index, count, order);

    polygon_t polygon;
    
    if (count > 1) {
        // polygons only have 1 outer ring,
        // so any extra vectors are considered to be
        // inner rings.
        polygon.inners().resize(count - 1);
    }

    auto outer = read_linestring(buffer, index, order);
    polygon.outer().reserve(outer.size());
    for (auto& p : outer) {
        polygon.outer().push_back(p);
    }

    for (uint32_t i = 0; count > 0 && i < count - 1; i++) {
        auto inner = read_linestring(buffer, index, order);
        polygon.inners().at(i).reserve(inner.size());
        for (auto& p : inner) {
            polygon.inners().at(i).push_back(p);
        }
    }

    return polygon;
}

// ----------------------------------------------------------------------------

typename wkb::multipoint_t wkb::read_multipoint(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    uint32_t count;
    utils::copy_from(buffer, index, count, order);

    multipoint_t mp;
    mp.resize(count);

    for (auto& point : mp) {
        const byte_t new_order = buffer[index];
        index++;

        uint32_t type;
        utils::copy_from(buffer, index, type, new_order);
        throw_if_not_type(type, wkb_geom_t::point);

        point = read_point(buffer, index, new_order);
    }

    return mp;
}

// ----------------------------------------------------------------------------

typename wkb::multilinestring_t wkb::read_multilinestring(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    uint32_t count;
    utils::copy_from(buffer, index, count, order);

    multilinestring_t ml;
    ml.resize(count);
    for (auto& line : ml) {
        const byte_t new_order = buffer[index];
        index++;

        uint32_t type;
        utils::copy_from(buffer, index, type, new_order);
        throw_if_not_type(type, wkb_geom_t::linestring);

        line = read_linestring(buffer, index, new_order);
    }

    return ml;
}

// ----------------------------------------------------------------------------

typename wkb::multipolygon_t wkb::read_multipolygon(const boost::span<const uint8_t> buffer, int& index, uint8_t order)
{
    uint32_t count;
    utils::copy_from(buffer, index, count, order);

    multipolygon_t mpl;
    mpl.resize(count);
    for (auto& polygon : mpl) {
        const byte_t new_order = buffer[index];
        index++;

        uint32_t type;
        utils::copy_from(buffer, index, type, new_order);
        throw_if_not_type(type, wkb_geom_t::polygon);

        polygon = read_polygon(buffer, index, new_order);
    }

    return mpl;
}

// ----------------------------------------------------------------------------

typename wkb::geometry wkb::read(const boost::span<const uint8_t> buffer)
{
    if (buffer.size() < 5) {
        throw std::runtime_error("buffer reached end before encountering WKB");
    }

    int index = 0;
    const byte_t order = buffer[index];
    index++;

    uint32_t type;
    utils::copy_from(buffer, index, type, order);

    switch(type) {
        case wkb_geom_t::point:
            return read_point(buffer, index, order);
        case wkb_geom_t::linestring:
            return read_linestring(buffer, index, order);
        case wkb_geom_t::polygon:
            return read_polygon(buffer, index, order);
        case wkb_geom_t::multipoint:
            return read_multipoint(buffer, index, order);
        case wkb_geom_t::multilinestring:
            return read_multilinestring(buffer, index, order);
        case wkb_geom_t::multipolygon:
            return read_multipolygon(buffer, index, order);
        default:
            throw std::runtime_error(
                "this reader only implements OGC geometry types 1-6, "
                "but received type " + std::to_string(type)
            );
    }
}

} // namespace geopackage
} // namespace ngen
