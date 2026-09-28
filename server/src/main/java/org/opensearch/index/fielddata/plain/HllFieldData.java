/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.metrics.AbstractHyperLogLogPlusPlus;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;

import static org.opensearch.search.aggregations.support.CoreValuesSourceType.BYTES;

/**
 * Field data implementation for HyperLogLog++ sketch fields.
 * Provides access to HLL++ sketches stored as binary doc values.
 *
 * @opensearch.internal
 */
public class HllFieldData implements IndexFieldData<HllFieldData.HllLeafFieldData> {

    /**
     * Builder for HLL field data
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final int precision;

        public Builder(String name, int precision) {
            this.name = name;
            this.precision = precision;
        }

        @Override
        public HllFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new HllFieldData(name, precision);
        }
    }

    private final String fieldName;
    private final int precision;

    private HllFieldData(String fieldName, int precision) {
        this.fieldName = fieldName;
        this.precision = precision;
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        // HLL fields use BYTES values source type since they store binary data
        return BYTES;
    }

    @Override
    public HllLeafFieldData load(LeafReaderContext context) {
        return new HllLeafFieldData(context.reader(), fieldName);
    }

    @Override
    public HllLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        throw new IllegalArgumentException("Sorting is not supported on [hll] fields");
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("Bucketed sort is not supported on [hll] fields");
    }

    /**
     * Leaf-level field data for HLL++ sketches.
     *
     * @opensearch.internal
     */
    public static class HllLeafFieldData implements LeafFieldData {

        private final LeafReader reader;
        private final String fieldName;

        HllLeafFieldData(LeafReader reader, String fieldName) {
            this.reader = reader;
            this.fieldName = fieldName;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public void close() {
            // Nothing to close
        }

        @Override
        public ScriptDocValues<?> getScriptValues() {
            throw new UnsupportedOperationException("HLL fields do not support getScriptValues");
        }

        @Override
        public SortedBinaryDocValues getBytesValues() {
            throw new UnsupportedOperationException("HLL fields do not support getBytesValues");
        }

        /**
         * Get the HLL++ sketch for the given document ID.
         *
         * @param docId the document ID
         * @return the HLL++ sketch, or null if the document doesn't have a value.
         * The caller is responsible for closing the returned sketch to release resources.
         * @throws IOException if an error occurs reading the sketch
         */
        public AbstractHyperLogLogPlusPlus getSketch(int docId) throws IOException {
            BinaryDocValues docValues = reader.getBinaryDocValues(fieldName);
            if (docValues != null && docValues.advanceExact(docId)) {
                BytesRef sketchBytes = docValues.binaryValue();
                return AbstractHyperLogLogPlusPlus.readFrom(
                    new BytesArray(sketchBytes.bytes, sketchBytes.offset, sketchBytes.length).streamInput(),
                    BigArrays.NON_RECYCLING_INSTANCE
                );
            }
            return null;
        }
    }
}
