/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.AbstractSortedDocValues;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.util.Collection;
import java.util.Collections;

/**
 * Constant value field data
 *
 * @opensearch.internal
 */
public class ConstantIndexFieldData extends AbstractIndexOrdinalsFieldData {

    /**
     * Builder for Constant Index Field Data
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {

        private final String constantValue;
        private final String name;
        private final ValuesSourceType valuesSourceType;

        public Builder(String constantValue, String name, ValuesSourceType valuesSourceType) {
            this.constantValue = constantValue;
            this.name = name;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public IndexFieldData<?> build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new ConstantIndexFieldData(name, constantValue, valuesSourceType);
        }
    }

    /**
     * Field data for constant values
     *
     * @opensearch.internal
     */
    private static class ConstantLeafFieldData extends AbstractLeafOrdinalsFieldData {

        private final String value;

        ConstantLeafFieldData(String value) {
            super(DEFAULT_SCRIPT_FUNCTION);
            this.value = value;
        }

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public SortedSetDocValues getOrdinalsValues() {
            if (value == null) {
                return DocValues.emptySortedSet();
            }
            final BytesRef term = new BytesRef(value);
            final SortedDocValues sortedValues = new AbstractSortedDocValues() {

                private int docID = -1;

                @Override
                public BytesRef lookupOrd(int ord) {
                    return term;
                }

                @Override
                public int getValueCount() {
                    return 1;
                }

                @Override
                public int ordValue() {
                    return 0;
                }

                @Override
                public boolean advanceExact(int target) {
                    docID = target;
                    return true;
                }

                @Override
                public int docID() {
                    return docID;
                }
            };
            return DocValues.singleton(sortedValues);
        }

        @Override
        public void close() {}

    }

    private final ConstantLeafFieldData atomicFieldData;

    private ConstantIndexFieldData(String name, String value, ValuesSourceType valuesSourceType) {
        super(name, valuesSourceType, null, null, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION);
        atomicFieldData = new ConstantLeafFieldData(value);
    }

    @Override
    public final LeafOrdinalsFieldData load(LeafReaderContext context) {
        return atomicFieldData;
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        return atomicFieldData;
    }

    @Override
    public SortField sortField(
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        boolean reverse
    ) {
        final XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        return new SortField(getFieldName(), source, reverse);
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
        throw new IllegalArgumentException("only supported on numeric fields");
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(DirectoryReader indexReader) {
        return this;
    }

    @Override
    public IndexOrdinalsFieldData loadGlobalDirect(DirectoryReader indexReader) {
        return loadGlobal(indexReader);
    }

    public String getValue() {
        return atomicFieldData.value;
    }

}
