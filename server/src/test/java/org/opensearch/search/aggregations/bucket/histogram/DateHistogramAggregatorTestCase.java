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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedBiConsumer;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public abstract class DateHistogramAggregatorTestCase extends AggregatorTestCase {
    /**
     * A date that is always "aggregable" because it has doc values but may or
     * may not have a search index. If it doesn't then we can't use our fancy
     * date rounding mechanism that needs to know the minimum and maximum dates
     * it is going to round because it ready *that* out of the search index.
     */
    protected static final String AGGREGABLE_DATE = "aggregable_date";

    protected final <R extends InternalAggregation> void asSubAggTestCase(AggregationBuilder builder, Consumer<R> verify)
        throws IOException {
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex = (iw, dft) -> {
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new SortedNumericDocValuesField("n", 1)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-03-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new SortedNumericDocValuesField("n", 2)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2021-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new SortedSetDocValuesField("k2", new BytesRef("a")),
                    new SortedNumericDocValuesField("n", 3)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2021-03-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("a")),
                    new SortedSetDocValuesField("k2", new BytesRef("b")),
                    new SortedNumericDocValuesField("n", 4)
                )
            );
            iw.addDocument(
                List.of(
                    new SortedNumericDocValuesField(AGGREGABLE_DATE, dft.parse("2020-02-01T00:00:00Z")),
                    new SortedSetDocValuesField("k1", new BytesRef("b")),
                    new SortedSetDocValuesField("k2", new BytesRef("b")),
                    new SortedNumericDocValuesField("n", 5)
                )
            );
        };
        asSubAggTestCase(builder, buildIndex, verify);
    }

    protected final <R extends InternalAggregation> void asSubAggTestCase(
        AggregationBuilder builder,
        CheckedBiConsumer<RandomIndexWriter, DateFieldMapper.DateFieldType, IOException> buildIndex,
        Consumer<R> verify
    ) throws IOException {
        KeywordFieldMapper.KeywordFieldType k1ft = new KeywordFieldMapper.KeywordFieldType("k1");
        KeywordFieldMapper.KeywordFieldType k2ft = new KeywordFieldMapper.KeywordFieldType("k2");
        NumberFieldMapper.NumberFieldType nft = new NumberFieldMapper.NumberFieldType("n", NumberType.LONG);
        DateFieldMapper.DateFieldType dft = aggregableDateFieldType(false, randomBoolean());
        testCase(builder, new MatchAllDocsQuery(), iw -> buildIndex.accept(iw, dft), verify, k1ft, k2ft, nft, dft);
    }

    protected final DateFieldMapper.DateFieldType aggregableDateFieldType(boolean useNanosecondResolution, boolean isSearchable) {
        return new DateFieldMapper.DateFieldType(
            AGGREGABLE_DATE,
            isSearchable,
            false,
            true,
            DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER,
            useNanosecondResolution ? DateFieldMapper.Resolution.NANOSECONDS : DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }
}
