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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.IndexReader;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.sort.SortOrder;

import java.util.function.LongConsumer;

public class CompositeValuesSourceConfig {

    @FunctionalInterface
    public interface SingleDimensionValuesSourceProvider {
        SingleDimensionValuesSource<?> createValuesSource(
            BigArrays bigArrays,
            IndexReader reader,
            int size,
            LongConsumer addRequestCircuitBreakerBytes,
            CompositeValuesSourceConfig config
        );
    }

    private final String name;
    @Nullable
    private final MappedFieldType fieldType;
    private final ValuesSource vs;
    private final DocValueFormat format;
    private final int reverseMul;
    private final boolean missingBucket;
    private final MissingOrder missingOrder;
    private final boolean hasScript;
    private final SingleDimensionValuesSourceProvider singleDimensionValuesSourceProvider;

    /**
     * Creates a new {@link CompositeValuesSourceConfig}.
     *
     * @param name The name of the source.
     * @param fieldType The field type or null if the source is a script.
     * @param vs The underlying {@link ValuesSource}.
     * @param format The {@link DocValueFormat} of this source.
     * @param order The sort order associated with this source.
     * @param missingBucket If <code>true</code> an explicit <code>null</code> bucket will represent documents with missing values.
     * @param hasScript <code>true</code> if the source contains a script that can change the value.
     */
    CompositeValuesSourceConfig(
        String name,
        @Nullable MappedFieldType fieldType,
        ValuesSource vs,
        DocValueFormat format,
        SortOrder order,
        boolean missingBucket,
        MissingOrder missingOrder,
        boolean hasScript,
        SingleDimensionValuesSourceProvider singleDimensionValuesSourceProvider
    ) {
        this.name = name;
        this.fieldType = fieldType;
        this.vs = vs;
        this.format = format;
        this.reverseMul = order == SortOrder.ASC ? 1 : -1;
        this.missingBucket = missingBucket;
        this.hasScript = hasScript;
        this.singleDimensionValuesSourceProvider = singleDimensionValuesSourceProvider;
        this.missingOrder = missingOrder;
    }

    /**
     * Returns the name associated with this configuration.
     */
    String name() {
        return name;
    }

    /**
     * Returns the {@link MappedFieldType} for this config.
     */
    MappedFieldType fieldType() {
        return fieldType;
    }

    /**
     * Returns the {@link ValuesSource} for this configuration.
     */
    ValuesSource valuesSource() {
        return vs;
    }

    /**
     * The {@link DocValueFormat} to use for formatting the keys.
     * {@link DocValueFormat#RAW} means no formatting.
     */
    DocValueFormat format() {
        return format;
    }

    /**
     * If true, an explicit `null bucket represents documents with missing values.
     */
    boolean missingBucket() {
        return missingBucket;
    }

    /**
     * Return the {@link MissingOrder} for the config.
     */
    MissingOrder missingOrder() {
        return missingOrder;
    }

    /**
     * Returns true if the source contains a script that can change the value.
     */
    boolean hasScript() {
        return hasScript;
    }

    /**
     * The sort order for the values source (e.g. -1 for descending and 1 for ascending).
     */
    int reverseMul() {
        assert reverseMul == -1 || reverseMul == 1;
        return reverseMul;
    }

    SingleDimensionValuesSource<?> createValuesSource(
        BigArrays bigArrays,
        IndexReader reader,
        int size,
        LongConsumer addRequestCircuitBreakerBytes
    ) {
        return this.singleDimensionValuesSourceProvider.createValuesSource(bigArrays, reader, size, addRequestCircuitBreakerBytes, this);
    }
}
