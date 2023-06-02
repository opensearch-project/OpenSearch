/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket.geogrid.cells;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.index.fielddata.GeoShapeValue;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.List;

/**
 * ValueSource class which converts the {@link GeoShapeValue} to numeric long values for bucketing. This class uses the
 * {@link GeoShapeCellIdSource.GeoShapeLongEncoder} to encode the geo_shape to {@link Long} values which can be iterated
 * to do the bucket aggregation.
 *
 * @opensearch.internal
 */
public class GeoShapeCellIdSource extends ValuesSource.Numeric {

    private final ValuesSource.GeoShape geoShape;
    private final int precision;
    private final GeoBoundingBox geoBoundingBox;
    private final GeoShapeCellIdSource.GeoShapeLongEncoder encoder;

    public GeoShapeCellIdSource(
        final ValuesSource.GeoShape geoShape,
        final int precision,
        final GeoBoundingBox geoBoundingBox,
        final GeoShapeCellIdSource.GeoShapeLongEncoder encoder
    ) {
        this.geoShape = geoShape;
        this.geoBoundingBox = geoBoundingBox;
        this.precision = precision;
        this.encoder = encoder;
    }

    /**
     * Get the current {@link SortedBinaryDocValues}.
     *
     * @param context {@link LeafReaderContext}
     */
    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
        throw new UnsupportedOperationException("The bytesValues operation is not supported on GeoShapeCellIdSource");
    }

    /**
     * Whether the underlying data is floating-point or not.
     */
    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    /**
     * Whether the underlying data is big integer or not.
     */
    @Override
    public boolean isBigInteger() {
        return false;
    }

    /**
     * Get the current {@link SortedNumericDocValues}.
     *
     * @param context {@link LeafReaderContext}
     */
    @Override
    public SortedNumericDocValues longValues(final LeafReaderContext context) {
        if (geoBoundingBox.isUnbounded()) {
            return new GeoShapeCellValues.UnboundedCellValues(geoShape.getGeoShapeValues(context), precision, encoder);
        }
        return new GeoShapeCellValues.BoundedCellValues(geoShape.getGeoShapeValues(context), precision, encoder, geoBoundingBox);
    }

    /**
     * Get the current {@link SortedNumericDoubleValues}.
     *
     * @param context {@link LeafReaderContext}
     */
    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext context) {
        throw new UnsupportedOperationException("The doubleValues operation is not supported on GeoShapeCellIdSource");
    }

    /**
     * Encoder to encode the GeoShapes to the specific long values for the aggregation.
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface GeoShapeLongEncoder {
        List<Long> encode(final GeoShapeDocValue geoShapeDocValue, final int precision);
    }
}
