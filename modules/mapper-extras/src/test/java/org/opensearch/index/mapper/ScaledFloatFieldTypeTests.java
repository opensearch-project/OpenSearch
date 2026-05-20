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

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.approximate.ApproximateScoreQuery;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class ScaledFloatFieldTypeTests extends FieldTypeTestCase {

    public void testTermQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        double value = (randomDouble() * 2 - 1) * 10000;
        long scaledValue = Math.round(value * ft.getScalingFactor());
        Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery("scaled_float", scaledValue);
        Query query = new IndexOrDocValuesQuery(LongPoint.newExactQuery("scaled_float", scaledValue), dvQuery);
        assertEquals(query, ft.termQuery(value, null));
    }

    public void testTermsQuery() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        double value1 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue1 = Math.round(value1 * ft.getScalingFactor());
        double value2 = (randomDouble() * 2 - 1) * 10000;
        long scaledValue2 = Math.round(value2 * ft.getScalingFactor());
        assertEquals(LongField.newSetQuery("scaled_float", scaledValue1, scaledValue2), ft.termsQuery(Arrays.asList(value1, value2), null));
    }

    public void testRangeQuery() throws IOException {
        // make sure the accuracy loss of scaled floats only occurs at index time
        // this test checks that searching scaled floats yields the same results as
        // searching doubles that are rounded to the closest half float
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            true,
            false,
            false,
            false,
            Collections.emptyMap(),
            0.1 + randomDouble() * 100,
            null
        );
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        long[] scaled_floats = new long[1000];
        double[] doubles = new double[1000];
        final int numDocs = 1000;
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            double value = (randomDouble() * 2 - 1) * 10000;
            long scaledValue = Math.round(value * ft.getScalingFactor());
            double rounded = scaledValue / ft.getScalingFactor();
            scaled_floats[i] = scaledValue;
            doubles[i] = rounded;
            doc.add(new LongPoint("scaled_float", scaledValue));
            doc.add(new DoublePoint("double", rounded));
            w.addDocument(doc);
        }
        final DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        final int numQueries = 1000;
        for (int i = 0; i < numQueries; ++i) {
            Double l = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            Double u = randomBoolean() ? null : (randomDouble() * 2 - 1) * 10000;
            boolean includeLower = randomBoolean();
            boolean includeUpper = randomBoolean();

            // Use the same rounding logic for query bounds as used in indexing
            Double queryL = l;
            Double queryU = u;
            if (l != null) {
                long scaledL = Math.round(l * ft.getScalingFactor());
                queryL = scaledL / ft.getScalingFactor();
            }
            if (u != null) {
                long scaledU = Math.round(u * ft.getScalingFactor());
                queryU = scaledU / ft.getScalingFactor();
            }

            Query doubleQ = NumberFieldMapper.NumberType.DOUBLE.rangeQuery(
                "double",
                queryL,
                queryU,
                includeLower,
                includeUpper,
                false,
                true,
                MOCK_QSC
            );
            Query scaledFloatQ = ft.rangeQuery(queryL, queryU, includeLower, includeUpper, MOCK_QSC);
            int expectedCount = searcher.count(doubleQ);
            int scaledCount = searcher.count(scaledFloatQ);

            // System.out.println("l=" + l + " queryL=" + queryL + " u=" + u + " queryU=" + queryU + " scalingFactor=" +
            // ft.getScalingFactor() + " expected= "+ expectedCount + " count= " + scaledCount);
            assertEquals(expectedCount, scaledCount);
        }
        IOUtils.close(reader, dir);
    }

    public void testRoundsUpperBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(null, 0.1, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 9]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 0.1, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 9]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 0.095, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, false, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 10]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 0.105, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 11]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(null, 79.99, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9223372036854775808 TO 7999]", getQueryString(scaledFloatQ));
    }

    public void testRoundsLowerBoundCorrectly() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Query scaledFloatQ = ft.rangeQuery(-0.1, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(-0.1, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-10 TO 9223372036854775807]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(-0.095, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-8 TO 9223372036854775807]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(-0.095, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(-0.105, null, false, true, MOCK_QSC);
        assertEquals("scaled_float:[-9 TO 9223372036854775807]", getQueryString(scaledFloatQ));
        scaledFloatQ = ft.rangeQuery(-0.105, null, true, true, MOCK_QSC);
        assertEquals("scaled_float:[-10 TO 9223372036854775807]", getQueryString(scaledFloatQ));
    }

    private String getQueryString(Query query) {
        assertTrue(query instanceof ApproximateScoreQuery);
        return ((IndexOrDocValuesQuery) ((ApproximateScoreQuery) query).getOriginalQuery()).getIndexQuery().toString();
    }

    public void testValueForSearch() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            0.1 + randomDouble() * 100
        );
        assertNull(ft.valueForDisplay(null));
        assertEquals(10 / ft.getScalingFactor(), ft.valueForDisplay(10L));
    }

    public void testFieldData() throws IOException {
        double scalingFactor = 0.1 + randomDouble() * 100;
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("scaled_float1", 10));
        doc.add(new SortedNumericDocValuesField("scaled_float2", 5));
        doc.add(new SortedNumericDocValuesField("scaled_float2", 12));
        w.addDocument(doc);
        try (DirectoryReader reader = DirectoryReader.open(w)) {
            // single-valued
            ScaledFloatFieldMapper.ScaledFloatFieldType f1 = new ScaledFloatFieldMapper.ScaledFloatFieldType(
                "scaled_float1",
                scalingFactor
            );
            IndexNumericFieldData fielddata = (IndexNumericFieldData) f1.fielddataBuilder(
                "index",
                () -> { throw new UnsupportedOperationException(); }
            ).build(null, null);
            assertEquals(fielddata.getNumericType(), IndexNumericFieldData.NumericType.DOUBLE);
            LeafNumericFieldData leafFieldData = fielddata.load(reader.leaves().get(0));
            SortedNumericDoubleValues values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(1, values.docValueCount());
            assertEquals(10 / f1.getScalingFactor(), values.nextValue(), 10e-5);

            // multi-valued
            ScaledFloatFieldMapper.ScaledFloatFieldType f2 = new ScaledFloatFieldMapper.ScaledFloatFieldType(
                "scaled_float2",
                scalingFactor
            );
            fielddata = (IndexNumericFieldData) f2.fielddataBuilder("index", () -> { throw new UnsupportedOperationException(); })
                .build(null, null);
            leafFieldData = fielddata.load(reader.leaves().get(0));
            values = leafFieldData.getDoubleValues();
            assertTrue(values.advanceExact(0));
            assertEquals(2, values.docValueCount());
            assertEquals(5 / f2.getScalingFactor(), values.nextValue(), 10e-5);
            assertEquals(12 / f2.getScalingFactor(), values.nextValue(), 10e-5);
        }
        IOUtils.close(w, dir);
    }

    public void testFetchSourceValue() throws IOException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).build();
        Mapper.BuilderContext context = new Mapper.BuilderContext(settings, new ContentPath());

        MappedFieldType mapper = new ScaledFloatFieldMapper.Builder("field", false, false).scalingFactor(100).build(context).fieldType();
        assertEquals(Collections.singletonList(3.14), fetchSourceValue(mapper, 3.1415926));
        assertEquals(Collections.singletonList(3.14), fetchSourceValue(mapper, "3.1415"));
        assertEquals(Collections.emptyList(), fetchSourceValue(mapper, ""));

        MappedFieldType nullValueMapper = new ScaledFloatFieldMapper.Builder("field", false, false).scalingFactor(100)
            .nullValue(2.71)
            .build(context)
            .fieldType();
        assertEquals(Collections.singletonList(2.71), fetchSourceValue(nullValueMapper, ""));
    }

    public void testRandomPriceValues() {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("price", 100);
        Query q = ft.rangeQuery(null, 19.99, true, true, MOCK_QSC);
        assertEquals("price:[-9223372036854775808 TO 1999]", getQueryString(q));
        q = ft.rangeQuery(null, 99.99, true, true, MOCK_QSC);
        assertEquals("price:[-9223372036854775808 TO 9999]", getQueryString(q));
        q = ft.rangeQuery(null, 9.99, true, true, MOCK_QSC);
        assertEquals("price:[-9223372036854775808 TO 999]", getQueryString(q));
    }

    public void testIndexingQueryingConsistency() throws IOException {
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType("scaled_float", 100);
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        // Index the problematic value
        Document doc = new Document();
        double value = 79.99;
        long scaledValue = Math.round(value * 100);
        doc.add(new LongPoint("scaled_float", scaledValue));
        w.addDocument(doc);
        DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        // Range query should find it
        Query rangeQ = ft.rangeQuery(79.0, 80.0, true, true, MOCK_QSC);
        assertEquals(1, searcher.count(rangeQ));
        // Exact range should find it
        Query exactQ = ft.rangeQuery(value, value, true, true, MOCK_QSC);
        assertEquals(1, searcher.count(exactQ));
        IOUtils.close(reader, dir);
    }

    public void testLargeNumberIndexingAndQuerying() throws IOException {
        double largeValue = 92233720368547750.0;
        double scalingFactor = 100.0;
        ScaledFloatFieldMapper.ScaledFloatFieldType ft = new ScaledFloatFieldMapper.ScaledFloatFieldType(
            "scaled_float",
            true,
            false,
            true,
            true,
            Collections.emptyMap(),
            scalingFactor,
            null
        );
        Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(null));
        // Index the document with the large value
        Document doc = new Document();
        long scaledValue = Math.round(largeValue * scalingFactor);
        doc.add(new LongPoint("scaled_float", scaledValue));
        doc.add(new SortedNumericDocValuesField("scaled_float", scaledValue));
        w.addDocument(doc);
        // Add another doc with a different value to ensure we're finding the right one
        Document doc2 = new Document();
        double otherValue = 1000.0;
        long scaledValue2 = Math.round(otherValue * scalingFactor);
        doc2.add(new LongPoint("scaled_float", scaledValue2));
        doc2.add(new SortedNumericDocValuesField("scaled_float", scaledValue2));
        w.addDocument(doc2);
        DirectoryReader reader = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = newSearcher(reader);
        // Test 1: Term query should find the exact document
        Query termQuery = ft.termQuery(largeValue, MOCK_QSC);
        assertEquals("Term query should find exactly one document", 1, searcher.count(termQuery));
        // Test 2: Range query containing the value should find it
        Query rangeQuery = ft.rangeQuery(largeValue - 1, largeValue + 1, true, true, MOCK_QSC);
        assertEquals("Range query should find the large value", 1, searcher.count(rangeQuery));
        // Test 3: Exact range query (value to value) should find it
        Query exactRangeQuery = ft.rangeQuery(largeValue, largeValue, true, true, MOCK_QSC);
        assertEquals("Exact range query should find the document", 1, searcher.count(exactRangeQuery));
        // Test 4: Range query excluding the value should not find it
        Query exclusiveRangeQuery = ft.rangeQuery(largeValue, largeValue + 1, false, false, MOCK_QSC);
        assertEquals("Exclusive range should not find the document", 0, searcher.count(exclusiveRangeQuery));
        // Test 5: Terms query with multiple values should work
        Query termsQuery = ft.termsQuery(Arrays.asList(largeValue, otherValue), MOCK_QSC);
        assertEquals("Terms query should find both documents", 2, searcher.count(termsQuery));
        IOUtils.close(reader, dir);
    }
}
