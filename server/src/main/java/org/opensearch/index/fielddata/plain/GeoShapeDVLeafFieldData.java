/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.document.LatLonShapeDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.index.fielddata.GeoShapeValue;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * This is the class that converts the DocValue of GeoShape field which is stored in Binary form using
 * {@link LatLonShapeDocValuesField} to {@link GeoShapeDocValue}.
 *
 * @opensearch.internal
 */
public class GeoShapeDVLeafFieldData extends AbstractLeafGeoShapeFieldData {

    private final LeafReader reader;
    private final String fieldName;

    GeoShapeDVLeafFieldData(final LeafReader reader, String fieldName) {
        super();
        this.reader = reader;
        this.fieldName = fieldName;
    }

    /**
     * Return the memory usage of this object in bytes. Negative values are illegal.
     */
    @Override
    public long ramBytesUsed() {
        return 0; // not exposed by lucene
    }

    @Override
    public void close() {
        // noop
    }

    /**
     * Returns nested resources of this class. The result should be a point-in-time snapshot (to avoid
     * race conditions).
     *
     * @see Accountables
     */
    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    /**
     * Reads the binary data from the {@link LeafReader} for a geo shape field and returns
     * {@link GeoShapeValue.StandardGeoShapeValue} instance which can be used to get the doc values from Lucene.
     *
     * @return {@link GeoShapeValue.StandardGeoShapeValue}
     */
    @Override
    public GeoShapeValue getGeoShapeValue() {
        try {
            // Using BinaryDocValues as LatLonShapeDocValuesField stores data in binary form.
            return new GeoShapeValue.StandardGeoShapeValue(DocValues.getBinary(reader, fieldName), fieldName);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load GeoShapeDocValues from lucene", e);
        }
    }
}
