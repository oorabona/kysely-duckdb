/**
 * DuckDB Spatial extension helpers
 */

import { type Expression, type RawBuilder, sql } from 'kysely'

/**
 * Spatial data types
 */
export type GeometryType =
  | 'POINT'
  | 'LINESTRING'
  | 'POLYGON'
  | 'MULTIPOINT'
  | 'MULTILINESTRING'
  | 'MULTIPOLYGON'
  | 'GEOMETRYCOLLECTION'

// Spatial Functions - Individual exports for better tree-shaking

/**
 * Create a point geometry from coordinates
 */
export function stPoint(
  x: number | Expression<number>,
  y: number | Expression<number>,
): RawBuilder<unknown> {
  return sql`ST_Point(${x}, ${y})`
}

/**
 * Get the geometry type
 */
export function stGeometryType(geom: Expression<unknown>): RawBuilder<string> {
  return sql`ST_GeometryType(${geom})`
}

/**
 * Check if geometry is valid
 */
export function stIsValid(geom: Expression<unknown>): RawBuilder<boolean> {
  return sql`ST_IsValid(${geom})`
}

/**
 * Get the area of a geometry
 */
export function stArea(geom: Expression<unknown>): RawBuilder<number> {
  return sql`ST_Area(${geom})`
}

/**
 * Get the length of a geometry
 */
export function stLength(geom: Expression<unknown>): RawBuilder<number> {
  return sql`ST_Length(${geom})`
}

/**
 * Check if two geometries intersect
 */
export function stIntersects(
  geom1: Expression<unknown>,
  geom2: Expression<unknown>,
): RawBuilder<boolean> {
  return sql`ST_Intersects(${geom1}, ${geom2})`
}

/**
 * Check if geometry is within another geometry
 */
export function stWithin(
  geom1: Expression<unknown>,
  geom2: Expression<unknown>,
): RawBuilder<boolean> {
  return sql`ST_Within(${geom1}, ${geom2})`
}

/**
 * Get the distance between two geometries
 */
export function stDistance(
  geom1: Expression<unknown>,
  geom2: Expression<unknown>,
): RawBuilder<number> {
  return sql`ST_Distance(${geom1}, ${geom2})`
}

/**
 * Create a buffer around a geometry
 */
export function stBuffer(
  geom: Expression<unknown>,
  distance: number | Expression<number>,
): RawBuilder<unknown> {
  return sql`ST_Buffer(${geom}, ${distance})`
}

/**
 * Convert geometry to WKT (Well-Known Text)
 */
export function stAsText(geom: Expression<unknown>): RawBuilder<string> {
  return sql`ST_AsText(${geom})`
}

/**
 * Convert geometry to GeoJSON
 */
export function stAsGeoJSON(geom: Expression<unknown>): RawBuilder<any> {
  return sql`ST_AsGeoJSON(${geom})`
}

/**
 * Create geometry from WKT
 */
export function stGeomFromText(wkt: string | Expression<string>): RawBuilder<unknown> {
  return sql`ST_GeomFromText(${wkt})`
}

/**
 * Create geometry from GeoJSON
 */
export function stGeomFromGeoJSON(geojson: string | Expression<string>): RawBuilder<unknown> {
  return sql`ST_GeomFromGeoJSON(${geojson})`
}

/**
 * Get the centroid of a geometry
 */
export function stCentroid(geom: Expression<unknown>): RawBuilder<unknown> {
  return sql`ST_Centroid(${geom})`
}

/**
 * Get the bounding box of a geometry
 */
export function stEnvelope(geom: Expression<unknown>): RawBuilder<unknown> {
  return sql`ST_Envelope(${geom})`
}

/**
 * Get the SRID (Spatial Reference System Identifier) of a geometry
 */
export function stSRID(geom: Expression<unknown>): RawBuilder<number> {
  return sql`ST_SRID(${geom})`
}

/**
 * Transform geometry to a different coordinate system
 */
export function stTransform(
  geom: Expression<unknown>,
  srid: number | Expression<number>,
): RawBuilder<unknown> {
  return sql`ST_Transform(${geom}, ${srid})`
}

// Backward compatibility - export object for existing code
export const SpatialFunctions = {
  stPoint,
  stGeometryType,
  stIsValid,
  stArea,
  stLength,
  stIntersects,
  stWithin,
  stDistance,
  stBuffer,
  stAsText,
  stAsGeoJSON,
  stGeomFromText,
  stGeomFromGeoJSON,
  stCentroid,
  stEnvelope,
  stSRID,
  stTransform,
} as const

/**
 * Helper to install and load the spatial extension
 */
export async function loadSpatialExtension(db: any): Promise<void> {
  await sql`INSTALL spatial`.execute(db)
  await sql`LOAD spatial`.execute(db)
}
