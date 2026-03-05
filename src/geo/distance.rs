// ============================================================================
// GraniteDB — Geospatial Distance Functions
// ============================================================================
// Haversine formula for great-circle distance on Earth's surface.
// ============================================================================

use crate::geo::types::GeoPoint;

/// Earth radius in meters.
pub const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

/// Compute the great-circle distance between two points using the Haversine formula.
/// Returns distance in meters.
pub fn haversine_distance(a: &GeoPoint, b: &GeoPoint) -> f64 {
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();
    let dlat = (b.lat - a.lat).to_radians();
    let dlon = (b.lon - a.lon).to_radians();

    let h = (dlat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);

    let c = 2.0 * h.sqrt().asin();

    EARTH_RADIUS_METERS * c
}

/// Convert meters to degrees (approximate, at equator).
pub fn meters_to_degrees(meters: f64) -> f64 {
    meters / 111_320.0
}

/// Convert degrees to meters (approximate, at equator).
pub fn degrees_to_meters(degrees: f64) -> f64 {
    degrees * 111_320.0
}

/// Check if a point is inside a polygon using the ray-casting algorithm.
pub fn point_in_polygon(point: &GeoPoint, polygon: &[GeoPoint]) -> bool {
    let mut inside = false;
    let n = polygon.len();
    let mut j = n - 1;

    for i in 0..n {
        if ((polygon[i].lat > point.lat) != (polygon[j].lat > point.lat))
            && (point.lon
                < (polygon[j].lon - polygon[i].lon) * (point.lat - polygon[i].lat)
                    / (polygon[j].lat - polygon[i].lat)
                    + polygon[i].lon)
        {
            inside = !inside;
        }
        j = i;
    }

    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine() {
        // New York to London: ~5570 km
        let ny = GeoPoint::new(-74.006, 40.7128);
        let london = GeoPoint::new(-0.1278, 51.5074);
        let dist = haversine_distance(&ny, &london);
        assert!((dist / 1000.0 - 5570.0).abs() < 100.0); // Within 100km
    }

    #[test]
    fn test_point_in_polygon() {
        let polygon = vec![
            GeoPoint::new(0.0, 0.0),
            GeoPoint::new(10.0, 0.0),
            GeoPoint::new(10.0, 10.0),
            GeoPoint::new(0.0, 10.0),
        ];
        assert!(point_in_polygon(&GeoPoint::new(5.0, 5.0), &polygon));
        assert!(!point_in_polygon(&GeoPoint::new(15.0, 5.0), &polygon));
    }
}
