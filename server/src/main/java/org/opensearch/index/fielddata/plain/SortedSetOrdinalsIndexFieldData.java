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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.util.function.Function;

/**
 * Sorted Set Ordinals doc values
 *
 * @opensearch.internal
 */
public class SortedSetOrdinalsIndexFieldData extends AbstractIndexOrdinalsFieldData {

    /**
     * Builder for sorted set ordinals
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this(name, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION, valuesSourceType);
        }

        public Builder(String name, Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.scriptFunction = scriptFunction;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public SortedSetOrdinalsIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SortedSetOrdinalsIndexFieldData(cache, name, valuesSourceType, breakerService, scriptFunction);
        }
    }

    public SortedSetOrdinalsIndexFieldData(
        IndexFieldDataCache cache,
        String fieldName,
        ValuesSourceType valuesSourceType,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        super(fieldName, valuesSourceType, cache, breakerService, scriptFunction);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        /**
         * Check if we can use a simple {@link SortedSetSortField} compatible with index sorting and
         * returns a custom sort field otherwise.
         */
        if (nested != null
            || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
            || (source.sortMissingLast(missingValue) == false && source.sortMissingFirst(missingValue) == false)) {
            return new SortField(getFieldName(), source, reverse);
        }
        SortField sortField = new SortedSetSortField(
            getFieldName(),
            reverse,
            sortMode == MultiValueMode.MAX ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN
        );
        sortField.setMissingValue(
            source.sortMissingLast(missingValue) ^ reverse ? SortedSetSortField.STRING_LAST : SortedSetSortField.STRING_FIRST
        );
        return sortField;
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
    public LeafOrdinalsFieldData load(LeafReaderContext context) {
        return new SortedSetBytesLeafFieldData(context.reader(), getFieldName(), scriptFunction);
    }

    @Override
    public LeafOrdinalsFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    @Override
    public OrdinalMap getOrdinalMap() {
        return null;
    }

    @Override
    public boolean supportsGlobalOrdinalsMapping() {
        return true;
    }
}
