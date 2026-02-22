#pragma once
#include "Point.hpp"
#include <vector>

namespace tip::model {

    /// Test whether segment (p1 -> p2) intersects segment (p3 -> p4).
    /// Uses the standard cross-product orientation method.
    /// Returns true for proper intersections (excludes shared endpoints).
    [[nodiscard]] inline bool segmentsIntersect(
        const Point& p1, const Point& p2,
        const Point& p3, const Point& p4) noexcept
    {
        auto cross = [](double ax, double ay, double bx, double by) -> double {
            return ax * by - ay * bx;
        };

        double d1x = p2.x - p1.x, d1y = p2.y - p1.y;
        double d2x = p4.x - p3.x, d2y = p4.y - p3.y;

        double denom = cross(d1x, d1y, d2x, d2y);

        // Parallel or collinear — treat as no conflict
        if (denom == 0.0) return false;

        double dx = p3.x - p1.x, dy = p3.y - p1.y;
        double t = cross(dx, dy, d2x, d2y) / denom;
        double u = cross(dx, dy, d1x, d1y) / denom;

        // Strict interior intersection (0, 1) exclusive to ignore shared endpoints
        constexpr double eps = 1e-9;
        return (t > eps && t < 1.0 - eps && u > eps && u < 1.0 - eps);
    }

    /// Test whether two polylines (sequences of points) have any intersecting segments.
    [[nodiscard]] inline bool polylinesIntersect(
        const std::vector<Point>& a,
        const std::vector<Point>& b) noexcept
    {
        if (a.size() < 2 || b.size() < 2) return false;

        for (std::size_t i = 0; i + 1 < a.size(); ++i) {
            for (std::size_t j = 0; j + 1 < b.size(); ++j) {
                if (segmentsIntersect(a[i], a[i + 1], b[j], b[j + 1])) {
                    return true;
                }
            }
        }
        return false;
    }

}