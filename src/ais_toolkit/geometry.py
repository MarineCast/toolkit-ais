"""Bounded spherical gnomonic geometry; great-circle edges become straight chords."""

from dataclasses import dataclass
from math import asin, atan, atan2, cos, degrees, hypot, isfinite, radians, sin, sqrt, tan

import h3
from shapely import LineString, Point, Polygon, STRtree, get_coordinates, union_all
from shapely.geometry import shape
from shapely.ops import transform

RADIUS_M = 6371007.180918475  # H3's WGS84 authalic sphere.


@dataclass(frozen=True)
class LocalProjection:
    longitude: float
    latitude: float
    radius_m: float
    max_relative_metric_error: float

    def __post_init__(self):
        if not all(not isinstance(v, bool) and isfinite(v) for v in vars(self).values()):
            raise ValueError("finite projection parameters required")
        if not -178 <= self.longitude <= 178 or not -74 <= self.latitude <= 74:
            raise ValueError("antimeridian/polar projection unsupported")
        if not 0 < self.radius_m <= 100000:
            raise ValueError("local spherical radius must be positive and at most 100 km")
        if not self.metric_error_bound <= self.max_relative_metric_error <= 0.02:
            raise ValueError("explicit metric error tolerance does not cover projection bound")

    @property
    def metric_error_bound(self):
        # WGS84 meridional minimum and transverse maximum bound all local directions.
        a, f = 6378137.0, 1 / 298.257223563
        e2 = f * (2 - f)
        low, high = a * (1 - e2), a / sqrt(1 - e2)
        sphere_error = max(abs(RADIUS_M / low - 1), abs(RADIUS_M / high - 1))
        return (1 + sphere_error) / cos(self.radius_m / RADIUS_M) ** 2 - 1

    def forward(self, lon, lat):
        if not isfinite(lon) or not isfinite(lat) or not -180 <= lon <= 180 or abs(lat) > 75:
            raise ValueError("unsupported WGS84 coordinate")
        if abs(lon - self.longitude) >= 180:
            raise ValueError("antimeridian crossing unsupported")
        phi, phi0, delta = radians(lat), radians(self.latitude), radians(lon - self.longitude)
        c = sin(phi0) * sin(phi) + cos(phi0) * cos(phi) * cos(delta)
        if c < cos(self.radius_m / RADIUS_M) - 1e-14:
            raise ValueError("coordinate outside bounded local projection")
        return (
            RADIUS_M * cos(phi) * sin(delta) / c,
            RADIUS_M * (cos(phi0) * sin(phi) - sin(phi0) * cos(phi) * cos(delta)) / c,
        )

    def inverse(self, x, y):
        if not isfinite(x) or not isfinite(y):
            raise ValueError("finite local coordinates required")
        rho = hypot(x, y)
        if rho > RADIUS_M * tan(self.radius_m / RADIUS_M) + 1e-7:
            raise ValueError("projected coordinate outside bounded local projection")
        if rho == 0:
            return self.longitude, self.latitude
        c, phi0 = atan(rho / RADIUS_M), radians(self.latitude)
        lat = asin(cos(c) * sin(phi0) + y * sin(c) * cos(phi0) / rho)
        lon = radians(self.longitude) + atan2(
            x * sin(c), rho * cos(phi0) * cos(c) - y * sin(phi0) * sin(c)
        )
        return degrees(lon), degrees(lat)


def polygonal(document, projection, max_vertices, *, allow_empty=False):
    geom = shape(document)
    if geom.geom_type not in ("Polygon", "MultiPolygon"):
        raise ValueError("polygon/multipolygon geometry required")
    if len(get_coordinates(geom)) > max_vertices:
        raise ValueError("geometry vertex cap exceeded")
    if (geom.is_empty and not allow_empty) or not geom.is_valid:
        raise ValueError("nonempty valid polygon geometry required")

    # Transform handles rings/holes; supplied edges have great-circle semantics.
    def project(x, y, z=None):
        if hasattr(x, "__iter__"):
            points = [projection.forward(lon, lat) for lon, lat in zip(x, y, strict=True)]
            return tuple(zip(*points, strict=True))
        return projection.forward(x, y)

    result = transform(project, geom)
    if not result.is_valid:
        raise ValueError("invalid projected geometry")
    return result


def cut_parameters(line, boundary):
    """All exact GEOS intersections, including endpoints of overlapping boundaries."""
    coordinates = get_coordinates(line.intersection(boundary))
    return [line.project(Point(x, y), normalized=True) for x, y in coordinates]


class LocalGeometry:
    """Explicit capped H3 tile; missing coverage is rejected, never classified absent."""

    def __init__(self, config, document):
        if set(document) != {
            "domain_version",
            "water_mask_version",
            "aoi",
            "land",
            "mask_support",
            "cells",
        }:
            raise ValueError("explicit geometry versions/AOI/land/support/cells required")
        if not all(
            isinstance(document[k], str) and document[k].strip()
            for k in ("domain_version", "water_mask_version")
        ):
            raise ValueError("geometry evidence versions required")
        self.config = config
        self.projection = LocalProjection(
            config.center_longitude,
            config.center_latitude,
            config.projection_radius_m,
            config.max_relative_metric_error,
        )
        self.aoi = polygonal(document["aoi"], self.projection, config.max_geometry_vertices)
        self.land = polygonal(
            document["land"], self.projection, config.max_geometry_vertices, allow_empty=True
        )
        self.support = polygonal(
            document["mask_support"], self.projection, config.max_geometry_vertices
        )
        cells = document["cells"]
        if not cells or len(cells) > config.max_cells or len(cells) != len(set(cells)):
            raise ValueError("unique nonempty explicit cells within cap required")
        self.cells = sorted(cells)
        self.polygons = []
        for cell in self.cells:
            if (
                not isinstance(cell, str)
                or not h3.is_valid_cell(cell)
                or h3.get_resolution(cell) != config.resolution
            ):
                raise ValueError("direct R6/R7 cells at selected resolution required")
            if h3.int_to_str(h3.str_to_int(cell)) != cell:
                raise ValueError("canonical lower-case H3 cell IDs required")
            coords = [self.projection.forward(lon, lat) for lat, lon in h3.cell_to_boundary(cell)]
            polygon = Polygon(coords)
            if not polygon.is_valid:
                raise ValueError("invalid projected H3 polygon")
            self.polygons.append(polygon)
        epsilon = config.numerical_tolerance_m
        if not union_all(self.polygons).buffer(epsilon).covers(self.aoi):
            raise ValueError("explicit H3 tile does not cover AOI")
        self.tree = STRtree(self.polygons)
        self.land_interior = self.land.buffer(-config.shoreline_uncertainty_m)
        self.land_uncertain = (
            self.land.buffer(config.shoreline_uncertainty_m)
            if not self.land.is_empty
            else self.land
        )

    def route(self, left, right):
        a = self.projection.forward(left["longitude"], left["latitude"])
        b = self.projection.forward(right["longitude"], right["latitude"])
        return Point(a) if a == b else LineString([a, b])

    def screen(self, route):
        epsilon = self.config.numerical_tolerance_m
        if not self.support.buffer(epsilon).covers(route):
            return "water_mask_unavailable"
        interior = route.intersection(self.land_interior)
        if not interior.is_empty and (
            route.geom_type == "Point" or interior.length >= self.config.material_land_crossing_m
        ):
            return "material_land_crossing"
        if route.intersects(self.land_uncertain):
            return "shoreline_uncertainty"
        return None

    def owner(self, point, candidates):
        epsilon = self.config.numerical_tolerance_m
        owners = [i for i in candidates if self.polygons[i].distance(point) <= epsilon]
        if not owners:
            raise ValueError("uncovered H3 geometry in AOI")
        return self.cells[min(owners)]  # sorted IDs: deterministic shared-edge/corner ownership.

    def spatial_parts(self, route):
        """Return a full [0,1] partition, cell=None for every outside-AOI remainder."""
        candidates = sorted(
            self.tree.query(route.buffer(self.config.numerical_tolerance_m)).tolist()
        )
        if len(candidates) > self.config.max_candidates:
            raise ValueError("spatial candidate cap exceeded")
        if route.geom_type == "Point":
            cell = self.owner(route, candidates) if self.aoi.covers(route) else None
            return [(0.0, 1.0, cell)]
        cuts = [0.0, 1.0, *cut_parameters(route, self.aoi.boundary)]
        for i in candidates:
            cuts.extend(cut_parameters(route, self.polygons[i].boundary))
        cuts = sorted(set(cuts))
        merged = [cuts[0]]
        for value in cuts[1:-1]:
            if (value - merged[-1]) * route.length > self.config.numerical_tolerance_m:
                merged.append(value)
        merged.append(1.0)
        parts = []
        for a, b in zip(merged, merged[1:]):
            if b <= a:
                continue
            point = route.interpolate((a + b) / 2, normalized=True)
            cell = self.owner(point, candidates) if self.aoi.covers(point) else None
            parts.append((a, b, cell))
        return parts
