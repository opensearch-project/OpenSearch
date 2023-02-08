/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket.geogrid.cells;

import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geometry.Rectangle;
import org.opensearch.index.fielddata.AbstractSortingNumericDocValues;
import org.opensearch.index.fielddata.GeoShapeValue;

import java.io.IOException;
import java.util.List;

/**
 * Class representing the long-encoded grid-cells belonging to the geoshape-doc-values. Class must encode the values
 * as long and then sort them in order to account for the cells correctly.
 *
 * @opensearch.internal
 */
abstract class GeoShapeCellValues extends AbstractSortingNumericDocValues {
    private final GeoShapeValue geoShapeValue;
    protected int precision;
    protected final GeoShapeCellIdSource.GeoShapeLongEncoder encoder;

    public GeoShapeCellValues(GeoShapeValue geoShapeValue, int precision, GeoShapeCellIdSource.GeoShapeLongEncoder encoder) {
        this.geoShapeValue = geoShapeValue;
        this.precision = precision;
        this.encoder = encoder;
    }

    @Override
    public boolean advanceExact(int docId) throws IOException {
        if (geoShapeValue.advanceExact(docId)) {
            final GeoShapeDocValue geoShapeDocValue = geoShapeValue.nextValue();
            relateShape(geoShapeDocValue);
            sort();
            return true;
        }
        return false;
    }

    /**
     * This function relates the shape's with the grid, and then put the intersecting grid's info as long, which
     * can be iterated in the aggregation. It uses the encoder to find the relation.
     *
     * @param geoShapeDocValue {@link GeoShapeDocValue}
     */
    abstract void relateShape(final GeoShapeDocValue geoShapeDocValue);

    /**
     * Provides the {@link GeoShapeCellValues} for the input bounding box.
     * @opensearch.internal
     */
    static class BoundedCellValues extends GeoShapeCellValues {
        private final Rectangle geoBoundingBox;

        public BoundedCellValues(
            final GeoShapeValue geoShapeValue,
            final int precision,
            final GeoShapeCellIdSource.GeoShapeLongEncoder encoder,
            final GeoBoundingBox boundingBox
        ) {
            super(geoShapeValue, precision, encoder);
            this.geoBoundingBox = new Rectangle(boundingBox.left(), boundingBox.right(), boundingBox.top(), boundingBox.bottom());
        }

        /**
         * This function relates the shape's with the grid, and then put the intersecting grid's info as long, which
         * can be iterated in the aggregation. It uses the encoder to find the relation.
         *
         * @param geoShapeDocValue {@link GeoShapeDocValue}
         */
        @Override
        void relateShape(final GeoShapeDocValue geoShapeDocValue) {
            if (geoShapeDocValue.isIntersectingRectangle(geoBoundingBox)) {
                // now we know the shape is in the bounding rectangle, we need add them in longValues
                // generate all grid that this shape intersects
                final List<Long> encodedValues = encoder.encode(geoShapeDocValue, precision);
                resize(encodedValues.size());
                for (int i = 0; i < encodedValues.size(); i++) {
                    values[i] = encodedValues.get(i);
                }
            } else {
                // As the shape is not intersecting with GeoBounding box, we need to reset the GeoShapeCellValues
                // calling this function resets the CellValues for the current shape.
                resize(0);
            }
        }

    }

    /**
     * Provides the {@link GeoShapeCellValues} for unbounded cells
     * @opensearch.internal
     */
    static class UnboundedCellValues extends GeoShapeCellValues {

        public UnboundedCellValues(
            final GeoShapeValue geoShapeValue,
            final int precision,
            final GeoShapeCellIdSource.GeoShapeLongEncoder encoder
        ) {
            super(geoShapeValue, precision, encoder);
        }

        /**
         * This function relates the shape's with the grid, and then put the intersecting grid's info as long, which
         * can be iterated in the aggregation. It uses the encoder to find the relation.
         *
         * @param geoShapeDocValue {@link GeoShapeDocValue}
         */
        @Override
        void relateShape(final GeoShapeDocValue geoShapeDocValue) {
            final List<Long> encodedValues = encoder.encode(geoShapeDocValue, precision);
            resize(encodedValues.size());
            for (int i = 0; i < encodedValues.size(); i++) {
                values[i] = encodedValues.get(i);
            }
        }
    }
}
