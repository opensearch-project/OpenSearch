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

package org.opensearch.search.aggregations.matrix.stats;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.matrix.MatrixAggregationModulePlugin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MatrixStatsAggregatorTests extends AggregatorTestCase {

    public void testNoData() throws Exception {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            if (randomBoolean()) {
                indexWriter.addDocument(Collections.singleton(new StringField("another_field", "value", Field.Store.NO)));
            }
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg").fields(
                    Collections.singletonList("field")
                );
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                assertNull(stats.getStats());
                assertEquals(0L, stats.getDocCount());
            }
        }
    }

    public void testUnmapped() throws Exception {
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            if (randomBoolean()) {
                indexWriter.addDocument(Collections.singleton(new StringField("another_field", "value", Field.Store.NO)));
            }
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg").fields(
                    Collections.singletonList("bogus")
                );
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ft);
                assertNull(stats.getStats());
                assertEquals(0L, stats.getDocCount());
            }
        }
    }

    public void testTwoFields() throws Exception {
        String fieldA = "a";
        MappedFieldType ftA = new NumberFieldMapper.NumberFieldType(fieldA, NumberFieldMapper.NumberType.DOUBLE);
        String fieldB = "b";
        MappedFieldType ftB = new NumberFieldMapper.NumberFieldType(fieldB, NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {

            int numDocs = scaledRandomIntBetween(8192, 16384);
            Double[] fieldAValues = new Double[numDocs];
            Double[] fieldBValues = new Double[numDocs];
            for (int docId = 0; docId < numDocs; docId++) {
                Document document = new Document();
                fieldAValues[docId] = randomDouble();
                document.add(new SortedNumericDocValuesField(fieldA, NumericUtils.doubleToSortableLong(fieldAValues[docId])));

                fieldBValues[docId] = randomDouble();
                document.add(new SortedNumericDocValuesField(fieldB, NumericUtils.doubleToSortableLong(fieldBValues[docId])));
                indexWriter.addDocument(document);
            }

            MultiPassStats multiPassStats = new MultiPassStats(fieldA, fieldB);
            multiPassStats.computeStats(Arrays.asList(fieldAValues), Arrays.asList(fieldBValues));
            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                MatrixStatsAggregationBuilder aggBuilder = new MatrixStatsAggregationBuilder("my_agg").fields(
                    Arrays.asList(fieldA, fieldB)
                );
                InternalMatrixStats stats = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, ftA, ftB);
                multiPassStats.assertNearlyEqual(stats);
                assertTrue(MatrixAggregationInspectionHelper.hasValue(stats));
            }
        }
    }

    public void testMultiValueModeAffectsResult() throws Exception {
        String field = "grades";
        MappedFieldType ft = new NumberFieldMapper.NumberFieldType(field, NumberFieldMapper.NumberType.DOUBLE);

        try (Directory directory = newDirectory(); RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(1.0)));
            doc.add(new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(3.0)));
            doc.add(new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(5.0)));
            indexWriter.addDocument(doc);

            try (IndexReader reader = indexWriter.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MatrixStatsAggregationBuilder avgAgg = new MatrixStatsAggregationBuilder("avg_agg").fields(Collections.singletonList(field))
                    .multiValueMode(MultiValueMode.AVG);

                MatrixStatsAggregationBuilder minAgg = new MatrixStatsAggregationBuilder("min_agg").fields(Collections.singletonList(field))
                    .multiValueMode(MultiValueMode.MIN);

                InternalMatrixStats avgStats = searchAndReduce(searcher, new MatchAllDocsQuery(), avgAgg, ft);
                InternalMatrixStats minStats = searchAndReduce(searcher, new MatchAllDocsQuery(), minAgg, ft);

                double avg = avgStats.getMean(field);
                double min = minStats.getMean(field);

                assertNotEquals("AVG and MIN mode should yield different means", avg, min, 0.0001);
            }
        }
    }

    public void testSerializationDeserialization() throws IOException {
        MatrixStatsAggregationBuilder original = new MatrixStatsAggregationBuilder("test").fields(Collections.singletonList("field"))
            .multiValueMode(MultiValueMode.MIN);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_1_0);
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_3_1_0);
        MatrixStatsAggregationBuilder deserialized = new MatrixStatsAggregationBuilder(in);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.fields(), deserialized.fields());
        assertEquals(original.multiValueMode(), deserialized.multiValueMode());
    }

    public void testDeserializationFallbackToAvg() throws IOException {
        MatrixStatsAggregationBuilder original = new MatrixStatsAggregationBuilder("test").fields(Collections.singletonList("field"));

        // Serialize with V_2_3_0 (fallback required)
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_2_3_0);
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        in.setVersion(Version.V_2_3_0);
        MatrixStatsAggregationBuilder deserialized = new MatrixStatsAggregationBuilder(in);

        assertEquals(MultiValueMode.AVG, deserialized.multiValueMode());
    }

    public void testEqualsAndHashCode() {
        MatrixStatsAggregationBuilder agg1 = new MatrixStatsAggregationBuilder("agg").fields(Collections.singletonList("field"))
            .multiValueMode(MultiValueMode.AVG);

        MatrixStatsAggregationBuilder agg2 = new MatrixStatsAggregationBuilder("agg").fields(Collections.singletonList("field"))
            .multiValueMode(MultiValueMode.AVG);

        MatrixStatsAggregationBuilder agg3 = new MatrixStatsAggregationBuilder("agg").fields(Collections.singletonList("field"))
            .multiValueMode(MultiValueMode.MIN);

        // equals
        assertEquals(agg1, agg2);
        assertNotEquals(agg1, agg3);

        // hashCode
        assertEquals(agg1.hashCode(), agg2.hashCode());
        assertNotEquals(agg1.hashCode(), agg3.hashCode());
    }

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new MatrixAggregationModulePlugin());
    }
}
