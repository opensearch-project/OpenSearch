/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.document.LatLonShapeDocValuesField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.mapper.GeoShapeFieldMapper;

import java.io.IOException;

/**
 * A stateful lightweight iterator interface to read the stored form of {@link Geometry} aka
 * {@link LatLonShapeDocValuesField} from Lucene per document. Check {@link GeoShapeFieldMapper} for details how we
 * converted the {@link Geometry} to {@link LatLonShapeDocValuesField}
 *
 * @opensearch.internal
 */
public abstract class GeoShapeValue {
    /**
     * Creates a new {@link GeoShapeValue} instance
     */
    protected GeoShapeValue() {}

    /**
     * Advance this instance to the given document id
     *
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Return the next value associated with the current document.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeoShapeDocValue nextValue() throws IOException;

    /**
     * This is the representation of an EmptyGeoShapeValue
     */
    public static class EmptyGeoShapeValue extends GeoShapeValue {
        /**
         * Advance this instance to the given document id
         *
         * @param doc int
         * @return true if there is a value for this document
         */
        @Override
        public boolean advanceExact(int doc) throws IOException {
            return false;
        }

        /**
         * Return the next value associated with the current document.
         *
         * @return the next value for the current docID set to {@link #advanceExact(int)}.
         */
        @Override
        public GeoShapeDocValue nextValue() throws IOException {
            throw new UnsupportedOperationException("This empty geoShape value, hence this operation is not supported");
        }
    }

    /**
     * The MissingGeoShapeValue is used when on a particular document the GeoShape field is not present and user has
     * provided a missing/default GeoShape value in the input which should be used.
     */
    public static class MissingGeoShapeValue extends GeoShapeValue {

        private boolean useMissingGeoShapeValue;
        private final GeoShapeValue valueSourceData;
        private final Geometry missing;

        private GeoShapeDocValue geoShapeDocValue;

        public MissingGeoShapeValue(final GeoShapeValue valueSourceData, final Geometry missing) {
            super();
            this.missing = missing;
            this.valueSourceData = valueSourceData;
            this.useMissingGeoShapeValue = false;
        }

        /**
         * Advance this instance to the given document id
         *
         * @param doc int
         * @return true if there is a value for this document
         */
        @Override
        public boolean advanceExact(int doc) throws IOException {
            // If we don't have next value for the doc then set useMissingGeoShapeValue = true
            useMissingGeoShapeValue = !valueSourceData.advanceExact(doc);
            // always return true because we want to return a value even if
            // the document does not have a value
            return true;
        }

        /**
         * Return the next value associated with the current document.
         *
         * @return the next value for the current docID set to {@link #advanceExact(int)}.
         */
        @Override
        public GeoShapeDocValue nextValue() throws IOException {
            if (useMissingGeoShapeValue) {
                if (geoShapeDocValue == null) {
                    // keeping geometryDocValue cache so that it can be reused.
                    geoShapeDocValue = GeoShapeDocValue.createGeometryDocValue(missing);
                }
                return geoShapeDocValue;
            }
            return valueSourceData.nextValue();
        }
    }

    /**
     * This is the standard implementation of the {@link GeoShapeValue} interface for iterating over the doc values
     * for a GeoShape field.
     */
    public static class StandardGeoShapeValue extends GeoShapeValue {

        private final BinaryDocValues binaryDocValues;
        private final String fieldName;

        public StandardGeoShapeValue(final BinaryDocValues binaryDocValues, final String fieldName) {
            this.binaryDocValues = binaryDocValues;
            this.fieldName = fieldName;
        }

        /**
         * Advance this instance to the given document id
         *
         * @return true if there is a value for this document
         */
        @Override
        public boolean advanceExact(int doc) throws IOException {
            return binaryDocValues.advanceExact(doc);
        }

        /**
         * Return the next value associated with the current document.
         *
         * @return the next value for the current docID set to {@link #advanceExact(int)}.
         */
        @Override
        public GeoShapeDocValue nextValue() throws IOException {
            final BytesRef bytesRef = binaryDocValues.binaryValue();
            // Converting the ByteRef to GeometryDocValue.
            return new GeoShapeDocValue(fieldName, bytesRef);
        }
    }
}
