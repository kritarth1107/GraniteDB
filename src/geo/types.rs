// ============================================================================
// GraniteDB — Geospatial Types
// ============================================================================

use serde::{Deserialize, Serialize};

/// A 2D point (longitude, latitude) — GeoJSON order.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

impl GeoPoint {
    pub fn new(lon: f64, lat: f64) -> Self {
        Self { lon, lat }
    }
}

/// A bounding box defined by its southwest and northeast corners.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BoundingBox {
    pub sw: GeoPoint,
    pub ne: GeoPoint,
}

impl BoundingBox {
    pub fn new(sw: GeoPoint, ne: GeoPoint) -> Self {
        Self { sw, ne }
    }

    pub fn contains(&self, point: &GeoPoint) -> bool {
        point.lon >= self.sw.lon
            && point.lon <= self.ne.lon
            && point.lat >= self.sw.lat
            && point.lat <= self.ne.lat
    }

    pub fn intersects(&self, other: &BoundingBox) -> bool {
        !(other.sw.lon > self.ne.lon
            || other.ne.lon < self.sw.lon
            || other.sw.lat > self.ne.lat
            || other.ne.lat < self.sw.lat)
    }

    pub fn center(&self) -> GeoPoint {
        GeoPoint {
            lon: (self.sw.lon + self.ne.lon) / 2.0,
            lat: (self.sw.lat + self.ne.lat) / 2.0,
        }
    }
}

/// GeoJSON geometry types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GeoShape {
    Point(GeoPoint),
    LineString(Vec<GeoPoint>),
    Polygon(Vec<Vec<GeoPoint>>),
    Circle { center: GeoPoint, radius_meters: f64 },
}

/// A geospatial query type.
#[derive(Debug, Clone)]
pub enum GeoQuery {
    /// Find documents near a point
    Near {
        center: GeoPoint,
        max_distance_meters: f64,
        min_distance_meters: Option<f64>,
    },
    /// Find documents within a bounding box
    WithinBox(BoundingBox),
    /// Find documents within a polygon
    WithinPolygon(Vec<GeoPoint>),
    /// Find documents within a circle
    WithinSphere {
        center: GeoPoint,
        radius_meters: f64,
    },
}
