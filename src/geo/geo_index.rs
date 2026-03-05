// ============================================================================
// GraniteDB — Geospatial Index (Grid-based)
// ============================================================================
// Grid-based spatial index for efficient $near, $geoWithin queries.
// Divides the world into cells and indexes points by cell ID.
// ============================================================================

use crate::geo::distance::{haversine_distance, point_in_polygon};
use crate::geo::types::{BoundingBox, GeoPoint, GeoQuery};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A geospatial index entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GeoEntry {
    doc_id: String,
    point: GeoPoint,
}

/// Grid-based geospatial index.
pub struct GeoIndex {
    /// Cell size in degrees
    cell_size: f64,
    /// Grid: cell_key → set of entries
    grid: HashMap<(i32, i32), Vec<GeoEntry>>,
    /// doc_id → point (for fast lookup)
    doc_points: HashMap<String, GeoPoint>,
}

impl GeoIndex {
    pub fn new() -> Self {
        Self {
            cell_size: 0.01, // ~1.1km cells
            grid: HashMap::new(),
            doc_points: HashMap::new(),
        }
    }

    /// With custom cell size in degrees.
    pub fn with_cell_size(cell_size: f64) -> Self {
        Self {
            cell_size,
            grid: HashMap::new(),
            doc_points: HashMap::new(),
        }
    }

    fn cell_key(&self, point: &GeoPoint) -> (i32, i32) {
        let x = (point.lon / self.cell_size).floor() as i32;
        let y = (point.lat / self.cell_size).floor() as i32;
        (x, y)
    }

    /// Insert a point into the index.
    pub fn insert(&mut self, doc_id: &str, point: GeoPoint) {
        self.remove(doc_id); // Remove old entry if exists

        let key = self.cell_key(&point);
        let entry = GeoEntry {
            doc_id: doc_id.to_string(),
            point,
        };

        self.grid.entry(key).or_default().push(entry);
        self.doc_points.insert(doc_id.to_string(), point);
    }

    /// Remove a document from the index.
    pub fn remove(&mut self, doc_id: &str) -> bool {
        if let Some(point) = self.doc_points.remove(doc_id) {
            let key = self.cell_key(&point);
            if let Some(entries) = self.grid.get_mut(&key) {
                entries.retain(|e| e.doc_id != doc_id);
                if entries.is_empty() {
                    self.grid.remove(&key);
                }
            }
            true
        } else {
            false
        }
    }

    /// Execute a geo query.
    pub fn query(&self, query: &GeoQuery) -> Vec<GeoSearchResult> {
        match query {
            GeoQuery::Near {
                center,
                max_distance_meters,
                min_distance_meters,
            } => self.near_query(center, *max_distance_meters, *min_distance_meters),
            GeoQuery::WithinBox(bbox) => self.within_box(bbox),
            GeoQuery::WithinPolygon(polygon) => self.within_polygon(polygon),
            GeoQuery::WithinSphere {
                center,
                radius_meters,
            } => self.near_query(center, *radius_meters, None),
        }
    }

    fn near_query(
        &self,
        center: &GeoPoint,
        max_dist: f64,
        min_dist: Option<f64>,
    ) -> Vec<GeoSearchResult> {
        // Calculate bounding cells
        let deg_range = max_dist / 111_320.0; // approximate degrees
        let min_lon = center.lon - deg_range;
        let max_lon = center.lon + deg_range;
        let min_lat = center.lat - deg_range;
        let max_lat = center.lat + deg_range;

        let min_cell_x = (min_lon / self.cell_size).floor() as i32;
        let max_cell_x = (max_lon / self.cell_size).ceil() as i32;
        let min_cell_y = (min_lat / self.cell_size).floor() as i32;
        let max_cell_y = (max_lat / self.cell_size).ceil() as i32;

        let mut results = Vec::new();

        for x in min_cell_x..=max_cell_x {
            for y in min_cell_y..=max_cell_y {
                if let Some(entries) = self.grid.get(&(x, y)) {
                    for entry in entries {
                        let dist = haversine_distance(center, &entry.point);
                        if dist <= max_dist {
                            if let Some(min) = min_dist {
                                if dist < min {
                                    continue;
                                }
                            }
                            results.push(GeoSearchResult {
                                doc_id: entry.doc_id.clone(),
                                distance_meters: dist,
                                point: entry.point,
                            });
                        }
                    }
                }
            }
        }

        results.sort_by(|a, b| {
            a.distance_meters
                .partial_cmp(&b.distance_meters)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        results
    }

    fn within_box(&self, bbox: &BoundingBox) -> Vec<GeoSearchResult> {
        let center = bbox.center();
        let mut results = Vec::new();

        let min_cell_x = (bbox.sw.lon / self.cell_size).floor() as i32;
        let max_cell_x = (bbox.ne.lon / self.cell_size).ceil() as i32;
        let min_cell_y = (bbox.sw.lat / self.cell_size).floor() as i32;
        let max_cell_y = (bbox.ne.lat / self.cell_size).ceil() as i32;

        for x in min_cell_x..=max_cell_x {
            for y in min_cell_y..=max_cell_y {
                if let Some(entries) = self.grid.get(&(x, y)) {
                    for entry in entries {
                        if bbox.contains(&entry.point) {
                            let dist = haversine_distance(&center, &entry.point);
                            results.push(GeoSearchResult {
                                doc_id: entry.doc_id.clone(),
                                distance_meters: dist,
                                point: entry.point,
                            });
                        }
                    }
                }
            }
        }

        results
    }

    fn within_polygon(&self, polygon: &[GeoPoint]) -> Vec<GeoSearchResult> {
        let mut results = Vec::new();

        // Calculate bounding box of polygon
        let min_lon = polygon.iter().map(|p| p.lon).fold(f64::MAX, f64::min);
        let max_lon = polygon.iter().map(|p| p.lon).fold(f64::MIN, f64::max);
        let min_lat = polygon.iter().map(|p| p.lat).fold(f64::MAX, f64::min);
        let max_lat = polygon.iter().map(|p| p.lat).fold(f64::MIN, f64::max);

        let min_cell_x = (min_lon / self.cell_size).floor() as i32;
        let max_cell_x = (max_lon / self.cell_size).ceil() as i32;
        let min_cell_y = (min_lat / self.cell_size).floor() as i32;
        let max_cell_y = (max_lat / self.cell_size).ceil() as i32;

        for x in min_cell_x..=max_cell_x {
            for y in min_cell_y..=max_cell_y {
                if let Some(entries) = self.grid.get(&(x, y)) {
                    for entry in entries {
                        if point_in_polygon(&entry.point, polygon) {
                            results.push(GeoSearchResult {
                                doc_id: entry.doc_id.clone(),
                                distance_meters: 0.0,
                                point: entry.point,
                            });
                        }
                    }
                }
            }
        }

        results
    }

    /// Number of indexed points.
    pub fn len(&self) -> usize {
        self.doc_points.len()
    }

    pub fn is_empty(&self) -> bool {
        self.doc_points.is_empty()
    }
}

/// A geospatial search result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoSearchResult {
    pub doc_id: String,
    pub distance_meters: f64,
    pub point: GeoPoint,
}
