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

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregator;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;

import java.io.IOException;
import java.util.function.BiConsumer;

import static java.util.Collections.singleton;

public class GlobalAggregatorTests extends AggregatorTestCase {
    public void testNoDocs() throws IOException {
        testCase(iw -> {
            // Intentionally not writing any docs
        }, (global, min) -> {
            assertEquals(0, global.getDocCount());
            assertEquals(Double.POSITIVE_INFINITY, min.getValue(), 0);
        });
    }

    public void testSomeDocs() throws IOException {
        testCase(iw -> {
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField("number", 7)));
            iw.addDocument(singleton(SortedNumericDocValuesField.indexedField("number", 1)));
        }, (global, min) -> {
            assertEquals(2, global.getDocCount());
            assertEquals(1, min.getValue(), 0);
        });
    }

    // Note that `global`'s fancy support for ignoring the query comes from special code in AggregationPhase. We don't test that here.

    private void testCase(CheckedConsumer<RandomIndexWriter, IOException> buildIndex, BiConsumer<InternalGlobal, InternalMin> verify)
        throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        buildIndex.accept(indexWriter);
        indexWriter.close();

        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader, true, true);

        GlobalAggregationBuilder aggregationBuilder = new GlobalAggregationBuilder("_name");
        aggregationBuilder.subAggregation(new MinAggregationBuilder("in_global").field("number"));
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        GlobalAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
        aggregator.preCollection();
        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        InternalGlobal result = (InternalGlobal) aggregator.buildTopLevel();
        verify.accept(result, (InternalMin) result.getAggregations().asMap().get("in_global"));

        indexReader.close();
        directory.close();
    }
}
