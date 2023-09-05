/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafGeoShapeFieldData;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

/**
 * Base class for retrieving Geometry docvalues
 *
 * @opensearch.internal
 */
public abstract class AbstractGeoShapeIndexFieldData implements IndexFieldData<LeafGeoShapeFieldData> {
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    AbstractGeoShapeIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
        this.fieldName = fieldName;
        this.valuesSourceType = valuesSourceType;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    /**
     * Returns the {@link SortField} to use for sorting.
     */
    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        throw new IllegalArgumentException("can't sort on geo_shape field without using specific sorting feature, like geo_distance");
    }

    /**
     * Build a sort implementation specialized for aggregations.
     */
    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("can't sort on geo_shape field without using specific sorting feature, like geo_distance");
    }

    /**
     * A concrete implementation of {@link AbstractGeoShapeIndexFieldData} which provides how to load the field data
     * aka Doc Values from Lucene.
     */
    public static class GeoShapeIndexFieldData extends AbstractGeoShapeIndexFieldData {

        public GeoShapeIndexFieldData(String fieldName, ValuesSourceType valuesSourceType) {
            super(fieldName, valuesSourceType);
        }

        /**
         * Loads the atomic field data for the reader, possibly cached.
         *
         * @param context {@link LeafReaderContext}
         */
        @Override
        public LeafGeoShapeFieldData load(LeafReaderContext context) {
            // do a compatibility check for the fieldName by getting the
            // filed info from the context.
            return new GeoShapeDVLeafFieldData(context.reader(), fieldName);
        }

        /**
         * Loads directly the atomic field data for the reader, ignoring any caching involved.
         *
         * @param context {@link LeafReaderContext}
         */
        @Override
        public LeafGeoShapeFieldData loadDirect(LeafReaderContext context) throws Exception {
            return load(context);
        }
    }

    /**
     * Builder class for creating the GeoShapeIndexFieldData.
     * This is required the way the indexfieldData is created via the builder class only.
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            // ignore breaker
            return new AbstractGeoShapeIndexFieldData.GeoShapeIndexFieldData(name, valuesSourceType);
        }
    }
}
