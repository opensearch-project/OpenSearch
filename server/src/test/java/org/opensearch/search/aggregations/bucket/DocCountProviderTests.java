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

package org.opensearch.search.aggregations.bucket;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Collections.singleton;

public class DocCountProviderTests extends AggregatorTestCase {

    private static final String DOC_COUNT_FIELD = DocCountFieldMapper.NAME;
    private static final String NUMBER_FIELD = "number";

    public void testDocsWithDocCount() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(List.of(new NumericDocValuesField(DOC_COUNT_FIELD, 4), new SortedNumericDocValuesField(NUMBER_FIELD, 1)));
            iw.addDocument(List.of(new NumericDocValuesField(DOC_COUNT_FIELD, 5), new SortedNumericDocValuesField(NUMBER_FIELD, 7)));
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new SortedNumericDocValuesField(NUMBER_FIELD, 1)
                )
            );
        }, global -> { assertEquals(10, global.getDocCount()); });
    }

    public void testDocsWithoutDocCount() throws IOException {
        testAggregation(new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField(NUMBER_FIELD, 1)));
        }, global -> { assertEquals(3, global.getDocCount()); });
    }

    public void testQueryFiltering() throws IOException {
        testAggregation(IntPoint.newRangeQuery(NUMBER_FIELD, 4, 5), iw -> {
            iw.addDocument(List.of(new NumericDocValuesField(DOC_COUNT_FIELD, 4), new IntPoint(NUMBER_FIELD, 6)));
            iw.addDocument(List.of(new NumericDocValuesField(DOC_COUNT_FIELD, 2), new IntPoint(NUMBER_FIELD, 5)));
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new IntPoint(NUMBER_FIELD, 1)
                )
            );
            iw.addDocument(
                List.of(
                    // Intentionally omit doc_count field
                    new IntPoint(NUMBER_FIELD, 5)
                )
            );
        }, global -> { assertEquals(3, global.getDocCount()); });
    }

    private void testAggregation(Query query, CheckedConsumer<RandomIndexWriter, IOException> indexer, Consumer<InternalGlobal> verify)
        throws IOException {
        GlobalAggregationBuilder aggregationBuilder = new GlobalAggregationBuilder("_name");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(NUMBER_FIELD, NumberFieldMapper.NumberType.LONG);
        MappedFieldType docCountFieldType = new DocCountFieldMapper.DocCountFieldType();
        testCase(aggregationBuilder, query, indexer, verify, fieldType, docCountFieldType);
    }
}
