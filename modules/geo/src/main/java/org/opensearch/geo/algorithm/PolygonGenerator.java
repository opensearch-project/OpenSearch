/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.algorithm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.common.util.CollectionUtils;

import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Helper class to generate a polygon. Keeping this in the src folder so that GeoSpatial plugin can take advantage of
 * this helper to create the Polygons, rather than hardcoding the values.
 */
public class PolygonGenerator {

    private static final Logger LOG = LogManager.getLogger(PolygonGenerator.class);

    /**
     * A helper function to create the Polygons for testing. The returned list of double array where first element
     * contains all the X points and second contains all the Y points.
     *
     * @param xPool a {@link java.util.List} of {@link Double}
     * @param yPool a {@link java.util.List} of {@link Double}
     * @return a {@link List} of double array.
     */
    public static List<double[]> generatePolygon(final List<Double> xPool, final List<Double> yPool, final Random random) {
        if (CollectionUtils.isEmpty(xPool) || CollectionUtils.isEmpty(yPool)) {
            LOG.debug("One of the X or Y list is empty or null. X.size : {} Y.size : {}", xPool, yPool);
            return Collections.emptyList();
        }
        final List<Point2D.Double> generatedPolygonPointsList = ValtrAlgorithm.generateRandomConvexPolygon(xPool, yPool, random);
        final double[] x = new double[generatedPolygonPointsList.size()];
        final double[] y = new double[generatedPolygonPointsList.size()];
        IntStream.range(0, generatedPolygonPointsList.size()).forEach(iterator -> {
            x[iterator] = generatedPolygonPointsList.get(iterator).getX();
            y[iterator] = generatedPolygonPointsList.get(iterator).getY();
        });
        final List<double[]> pointsList = new ArrayList<>();
        pointsList.add(x);
        pointsList.add(y);
        return pointsList;
    }

    /*
     * MIT License
     *
     * Copyright (c) 2017 Sander Verdonschot
     *
     * Permission is hereby granted, free of charge, to any person obtaining a copy
     * of this software and associated documentation files (the "Software"), to deal
     * in the Software without restriction, including without limitation the rights
     * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
     * copies of the Software, and to permit persons to whom the Software is
     * furnished to do so, subject to the following conditions:
     *
     * The above copyright notice and this permission notice shall be included in all
     * copies or substantial portions of the Software.
     *
     * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
     * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
     * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
     * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
     * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
     * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
     * SOFTWARE.
     */
    /**
     * Provides a helper function to create a Polygon with a list of points. This source code is used to create the
     * polygons in the test cases.
     * <a href="https://cglab.ca/~sander/misc/ConvexGeneration/ValtrAlgorithm.java">Reference Link</a>
     * <a href="https://observablehq.com/@tarte0/generate-random-simple-polygon">Visual Link</a>
     */
    private static class ValtrAlgorithm {
        /**
         * Generates a convex polygon using the points provided as a {@link List} of {@link Double} for both X and Y axis.
         *
         * @param xPool a {@link List} of {@link Double}
         * @param yPool a {@link List} of {@link Double}
         * @return a {@link List} of {@link Point2D.Double}
         */
        private static List<Point2D.Double> generateRandomConvexPolygon(
            final List<Double> xPool,
            final List<Double> yPool,
            final Random random
        ) {
            final int n = xPool.size();
            // Sort them
            Collections.sort(xPool);
            Collections.sort(yPool);

            // Isolate the extreme points
            final Double minX = xPool.get(0);
            final Double maxX = xPool.get(n - 1);
            final Double minY = yPool.get(0);
            final Double maxY = yPool.get(n - 1);

            // Divide the interior points into two chains & Extract the vector components
            java.util.List<Double> xVec = new ArrayList<>(n);
            java.util.List<Double> yVec = new ArrayList<>(n);

            double lastTop = minX, lastBot = minX;

            for (int i = 1; i < n - 1; i++) {
                double x = xPool.get(i);

                if (random.nextBoolean()) {
                    xVec.add(x - lastTop);
                    lastTop = x;
                } else {
                    xVec.add(lastBot - x);
                    lastBot = x;
                }
            }

            xVec.add(maxX - lastTop);
            xVec.add(lastBot - maxX);

            double lastLeft = minY, lastRight = minY;

            for (int i = 1; i < n - 1; i++) {
                double y = yPool.get(i);

                if (random.nextBoolean()) {
                    yVec.add(y - lastLeft);
                    lastLeft = y;
                } else {
                    yVec.add(lastRight - y);
                    lastRight = y;
                }
            }

            yVec.add(maxY - lastLeft);
            yVec.add(lastRight - maxY);

            // Randomly pair up the X- and Y-components
            Collections.shuffle(yVec, random);

            // Combine the paired up components into vectors
            List<Point2D.Double> vec = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                vec.add(new Point2D.Double(xVec.get(i), yVec.get(i)));
            }

            // Sort the vectors by angle
            Collections.sort(vec, Comparator.comparingDouble(v -> Math.atan2(v.getY(), v.getX())));

            // Lay them end-to-end
            double x = 0, y = 0;
            double minPolygonX = 0;
            double minPolygonY = 0;
            List<Point2D.Double> points = new ArrayList<>(n);

            for (int i = 0; i < n; i++) {
                points.add(new Point2D.Double(x, y));

                x += vec.get(i).getX();
                y += vec.get(i).getY();

                minPolygonX = Math.min(minPolygonX, x);
                minPolygonY = Math.min(minPolygonY, y);
            }

            // Move the polygon to the original min and max coordinates
            double xShift = minX - minPolygonX;
            double yShift = minY - minPolygonY;

            for (int i = 0; i < n; i++) {
                Point2D.Double p = points.get(i);
                points.set(i, new Point2D.Double(p.x + xShift, p.y + yShift));
            }

            return points;
        }
    }

}
