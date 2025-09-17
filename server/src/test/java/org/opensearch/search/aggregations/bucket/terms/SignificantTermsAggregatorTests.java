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

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.opensearch.index.mapper.NumberFieldMapper.NumberType;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper.TextFieldType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.bucket.terms.SignificantTermsAggregatorFactory.ExecutionMode;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.search.aggregations.AggregationBuilders.significantTerms;
import static org.hamcrest.Matchers.equalTo;

public class SignificantTermsAggregatorTests extends AggregatorTestCase {
    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new SignificantTermsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Arrays.asList(
            CoreValuesSourceType.NUMERIC,
            CoreValuesSourceType.BYTES,
            CoreValuesSourceType.BOOLEAN,
            CoreValuesSourceType.DATE,
            CoreValuesSourceType.IP
        );
    }

    @Override
    protected List<String> unsupportedMappedFieldTypes() {
        return Arrays.asList(
            NumberFieldMapper.NumberType.DOUBLE.typeName(), // floating points are not supported at all
            NumberFieldMapper.NumberType.FLOAT.typeName(),
            NumberFieldMapper.NumberType.HALF_FLOAT.typeName(),
            BinaryFieldMapper.CONTENT_TYPE // binary fields are not supported because they cannot be searched
        );
    }

    /**
     * For each provided field type, we also register an alias with name {@code <field>-alias}.
     */
    @Override
    protected Map<String, MappedFieldType> getFieldAliases(MappedFieldType... fieldTypes) {
        return Arrays.stream(fieldTypes).collect(Collectors.toMap(ft -> ft.name() + "-alias", Function.identity()));
    }

    /**
     * Uses the significant terms aggregation to find the keywords in text fields
     */
    public void testSignificance() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");
        textFieldType.setFielddata(true);
        textFieldType.setIndexAnalyzer(new NamedAnalyzer("my_analyzer", AnalyzerScope.GLOBAL, new StandardAnalyzer()));

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            addMixedTextDocs(w);

            SignificantTermsAggregationBuilder sigAgg = new SignificantTermsAggregationBuilder("sig_text").field("text");
            sigAgg.executionHint(randomExecutionHint());
            if (randomBoolean()) {
                // Use a background filter which just happens to be same scope as whole-index.
                sigAgg.backgroundFilter(QueryBuilders.termsQuery("text", "common"));
            }

            SignificantTermsAggregationBuilder sigNumAgg = new SignificantTermsAggregationBuilder("sig_number").field("long_field");
            sigNumAgg.executionHint(randomExecutionHint());

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                // Search "odd"
                SignificantStringTerms terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigAgg, textFieldType);

                assertThat(terms.getSubsetSize(), equalTo(5L));
                assertEquals(1, terms.getBuckets().size());
                assertNull(terms.getBucketByKey("even"));
                assertNull(terms.getBucketByKey("common"));
                assertNotNull(terms.getBucketByKey("odd"));

                // Search even
                terms = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), sigAgg, textFieldType);

                assertThat(terms.getSubsetSize(), equalTo(5L));
                assertEquals(1, terms.getBuckets().size());
                assertNull(terms.getBucketByKey("odd"));
                assertNull(terms.getBucketByKey("common"));
                assertNotNull(terms.getBucketByKey("even"));

                // Search odd with regex includeexcludes
                sigAgg.includeExclude(new IncludeExclude("o.d", null));
                terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigAgg, textFieldType);
                assertThat(terms.getSubsetSize(), equalTo(5L));
                assertEquals(1, terms.getBuckets().size());
                assertNotNull(terms.getBucketByKey("odd"));
                assertNull(terms.getBucketByKey("common"));
                assertNull(terms.getBucketByKey("even"));

                // Search with string-based includeexcludes
                String oddStrings[] = new String[] { "odd", "weird" };
                String evenStrings[] = new String[] { "even", "regular" };

                sigAgg.includeExclude(new IncludeExclude(oddStrings, evenStrings));
                sigAgg.significanceHeuristic(SignificanceHeuristicTests.getRandomSignificanceheuristic());
                terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigAgg, textFieldType);
                assertThat(terms.getSubsetSize(), equalTo(5L));
                assertEquals(1, terms.getBuckets().size());
                assertNotNull(terms.getBucketByKey("odd"));
                assertNull(terms.getBucketByKey("weird"));
                assertNull(terms.getBucketByKey("common"));
                assertNull(terms.getBucketByKey("even"));
                assertNull(terms.getBucketByKey("regular"));

                sigAgg.includeExclude(new IncludeExclude(evenStrings, oddStrings));
                terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigAgg, textFieldType);
                assertThat(terms.getSubsetSize(), equalTo(5L));
                assertEquals(0, terms.getBuckets().size());
                assertNull(terms.getBucketByKey("odd"));
                assertNull(terms.getBucketByKey("weird"));
                assertNull(terms.getBucketByKey("common"));
                assertNull(terms.getBucketByKey("even"));
                assertNull(terms.getBucketByKey("regular"));

            }
        }
    }

    /**
     * Uses the significant terms aggregation to find the keywords in numeric
     * fields
     */
    public void testNumericSignificance() throws IOException {
        NumberFieldType longFieldType = new NumberFieldMapper.NumberFieldType("long_field", NumberFieldMapper.NumberType.LONG);

        TextFieldType textFieldType = new TextFieldType("text");

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        final long ODD_VALUE = 3;
        final long EVEN_VALUE = 6;
        final long COMMON_VALUE = 2;

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {

            for (int i = 0; i < 10; i++) {
                Document doc = new Document();
                if (i % 2 == 0) {
                    addFields(doc, NumberType.LONG.createFields("long_field", ODD_VALUE, true, true, false, false));
                    doc.add(new Field("text", "odd", TextFieldMapper.Defaults.FIELD_TYPE));
                } else {
                    addFields(doc, NumberType.LONG.createFields("long_field", EVEN_VALUE, true, true, false, false));
                    doc.add(new Field("text", "even", TextFieldMapper.Defaults.FIELD_TYPE));
                }
                addFields(doc, NumberType.LONG.createFields("long_field", COMMON_VALUE, true, true, false, false));
                w.addDocument(doc);
            }

            SignificantTermsAggregationBuilder sigNumAgg = new SignificantTermsAggregationBuilder("sig_number").field("long_field");
            sigNumAgg.executionHint(randomExecutionHint());

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                // Search "odd"
                SignificantLongTerms terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigNumAgg, longFieldType);
                assertEquals(1, terms.getBuckets().size());
                assertThat(terms.getSubsetSize(), equalTo(5L));

                assertNull(terms.getBucketByKey(Long.toString(EVEN_VALUE)));
                assertNull(terms.getBucketByKey(Long.toString(COMMON_VALUE)));
                assertNotNull(terms.getBucketByKey(Long.toString(ODD_VALUE)));

                terms = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), sigNumAgg, longFieldType);
                assertEquals(1, terms.getBuckets().size());
                assertThat(terms.getSubsetSize(), equalTo(5L));

                assertNull(terms.getBucketByKey(Long.toString(ODD_VALUE)));
                assertNull(terms.getBucketByKey(Long.toString(COMMON_VALUE)));
                assertNotNull(terms.getBucketByKey(Long.toString(EVEN_VALUE)));

            }
        }
    }

    /**
     * Uses the significant terms aggregation on an index with unmapped field
     */
    public void testUnmapped() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");
        textFieldType.setFielddata(true);
        textFieldType.setIndexAnalyzer(new NamedAnalyzer("my_analyzer", AnalyzerScope.GLOBAL, new StandardAnalyzer()));

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            addMixedTextDocs(w);

            // Attempt aggregation on unmapped field
            SignificantTermsAggregationBuilder sigAgg = new SignificantTermsAggregationBuilder("sig_text").field("unmapped_field");
            sigAgg.executionHint(randomExecutionHint());

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                // Search "odd"
                SignificantTerms terms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), sigAgg, textFieldType);
                assertEquals(0, terms.getBuckets().size());

                assertNull(terms.getBucketByKey("even"));
                assertNull(terms.getBucketByKey("common"));
                assertNull(terms.getBucketByKey("odd"));

            }
        }
    }

    /**
     * Uses the significant terms aggregation on a range field
     */
    public void testRangeField() throws IOException {
        RangeType rangeType = RangeType.DOUBLE;
        final RangeFieldMapper.Range range1 = new RangeFieldMapper.Range(rangeType, 1.0D, 5.0D, true, true);
        final RangeFieldMapper.Range range2 = new RangeFieldMapper.Range(rangeType, 6.0D, 10.0D, true, true);
        final String fieldName = "rangeField";
        MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            for (RangeFieldMapper.Range range : new RangeFieldMapper.Range[] {
                new RangeFieldMapper.Range(rangeType, 1L, 5L, true, true),
                new RangeFieldMapper.Range(rangeType, -3L, 4L, true, true),
                new RangeFieldMapper.Range(rangeType, 4L, 13L, true, true),
                new RangeFieldMapper.Range(rangeType, 42L, 49L, true, true), }) {
                Document doc = new Document();
                BytesRef encodedRange = rangeType.encodeRanges(Collections.singleton(range));
                doc.add(new BinaryDocValuesField("field", encodedRange));
                w.addDocument(doc);
            }

            // Attempt aggregation on range field
            SignificantTermsAggregationBuilder sigAgg = new SignificantTermsAggregationBuilder("sig_text").field(fieldName);
            sigAgg.executionHint(randomExecutionHint());

            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher indexSearcher = newIndexSearcher(reader);
                expectThrows(IllegalArgumentException.class, () -> createAggregator(sigAgg, indexSearcher, fieldType));
            }
        }
    }

    public void testFieldAlias() throws IOException {
        TextFieldType textFieldType = new TextFieldType("text");
        textFieldType.setFielddata(true);
        textFieldType.setIndexAnalyzer(new NamedAnalyzer("my_analyzer", AnalyzerScope.GLOBAL, new StandardAnalyzer()));

        IndexWriterConfig indexWriterConfig = newIndexWriterConfig();
        indexWriterConfig.setMaxBufferedDocs(100);
        indexWriterConfig.setRAMBufferSizeMB(100); // flush on open to have a single segment

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, indexWriterConfig)) {
            addMixedTextDocs(w);

            SignificantTermsAggregationBuilder agg = significantTerms("sig_text").field("text");
            SignificantTermsAggregationBuilder aliasAgg = significantTerms("sig_text").field("text-alias");

            String executionHint = randomExecutionHint();
            agg.executionHint(executionHint);
            aliasAgg.executionHint(executionHint);

            if (randomBoolean()) {
                // Use a background filter which just happens to be same scope as whole-index.
                QueryBuilder backgroundFilter = QueryBuilders.termsQuery("text", "common");
                agg.backgroundFilter(backgroundFilter);
                aliasAgg.backgroundFilter(backgroundFilter);
            }

            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals("test expects a single segment", 1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);

                SignificantTerms evenTerms = searchAndReduce(searcher, new TermQuery(new Term("text", "even")), agg, textFieldType);
                SignificantTerms aliasEvenTerms = searchAndReduce(
                    searcher,
                    new TermQuery(new Term("text", "even")),
                    aliasAgg,
                    textFieldType
                );

                assertFalse(evenTerms.getBuckets().isEmpty());
                assertEquals(evenTerms, aliasEvenTerms);

                SignificantTerms oddTerms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), agg, textFieldType);
                SignificantTerms aliasOddTerms = searchAndReduce(searcher, new TermQuery(new Term("text", "odd")), aliasAgg, textFieldType);

                assertFalse(oddTerms.getBuckets().isEmpty());
                assertEquals(oddTerms, aliasOddTerms);
            }
        }
    }

    public void testAllDocsWithoutStringFieldviaGlobalOrds() throws IOException {
        testAllDocsWithoutStringField("global_ordinals");
    }

    public void testAllDocsWithoutStringFieldViaMap() throws IOException {
        testAllDocsWithoutStringField("map");
    }

    /**
     * Make sure that when the field is mapped but there aren't any values
     * for it we return a properly shaped "empty" result. In particular, the
     * {@link InternalMappedSignificantTerms#getSubsetSize()} needs to be set
     * to the number of matching documents.
     */
    private void testAllDocsWithoutStringField(String executionHint) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("f").field("f")
                        .executionHint(executionHint);
                    SignificantStringTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), request, keywordField("f"));
                    assertThat(result.getSubsetSize(), equalTo(1L));
                }
            }
        }
    }

    /**
     * Make sure that when the field is mapped but there aren't any values
     * for it we return a properly shaped "empty" result. In particular, the
     * {@link InternalMappedSignificantTerms#getSubsetSize()} needs to be set
     * to the number of matching documents.
     */
    public void testAllDocsWithoutNumericField() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                writer.addDocument(new Document());
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("f").field("f");
                    SignificantLongTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), request, longField("f"));
                    assertThat(result.getSubsetSize(), equalTo(1L));
                }
            }
        }
    }

    public void testSomeDocsWithoutStringFieldviaGlobalOrds() throws IOException {
        testSomeDocsWithoutStringField("global_ordinals");
    }

    public void testSomeDocsWithoutStringFieldViaMap() throws IOException {
        testSomeDocsWithoutStringField("map");
    }

    /**
     * Make sure that when the field a segment doesn't contain the field we
     * still include the count of its matching documents
     * in {@link InternalMappedSignificantTerms#getSubsetSize()}.
     */
    private void testSomeDocsWithoutStringField(String executionHint) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document d = new Document();
                d.add(new SortedDocValuesField("f", new BytesRef("f")));
                writer.addDocument(d);
                writer.flush();
                writer.addDocument(new Document());
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("f").field("f")
                        .executionHint(executionHint);
                    SignificantStringTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), request, keywordField("f"));
                    assertThat(result.getSubsetSize(), equalTo(2L));
                }
            }
        }
    }

    /**
     * Make sure that when the field a segment doesn't contain the field we
     * still include the count of its matching documents
     * in {@link InternalMappedSignificantTerms#getSubsetSize()}.
     */
    public void testSomeDocsWithoutNumericField() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                Document d = new Document();
                d.add(new SortedNumericDocValuesField("f", 1));
                writer.addDocument(d);
                writer.addDocument(new Document());
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("f").field("f");
                    SignificantLongTerms result = searchAndReduce(searcher, new MatchAllDocsQuery(), request, longField("f"));
                    assertThat(result.getSubsetSize(), equalTo(2L));
                }
            }
        }
    }

    public void testThreeLayerStringViaGlobalOrds() throws IOException {
        threeLayerStringTestCase("global_ordinals");
    }

    public void testThreeLayerStringViaMap() throws IOException {
        threeLayerStringTestCase("map");
    }

    private void threeLayerStringTestCase(String executionHint) throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < 10; i++) {
                    for (int j = 0; j < 10; j++) {
                        for (int k = 0; k < 10; k++) {
                            Document d = new Document();
                            d.add(new SortedDocValuesField("i", new BytesRef(Integer.toString(i))));
                            d.add(new SortedDocValuesField("j", new BytesRef(Integer.toString(j))));
                            d.add(new SortedDocValuesField("k", new BytesRef(Integer.toString(k))));
                            writer.addDocument(d);
                        }
                    }
                }
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder kRequest = new SignificantTermsAggregationBuilder("k").field("k")
                        .minDocCount(0)
                        .executionHint(executionHint);
                    SignificantTermsAggregationBuilder jRequest = new SignificantTermsAggregationBuilder("j").field("j")
                        .minDocCount(0)
                        .executionHint(executionHint)
                        .subAggregation(kRequest);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("i").field("i")
                        .minDocCount(0)
                        .executionHint(executionHint)
                        .subAggregation(jRequest);
                    SignificantStringTerms result = searchAndReduce(
                        searcher,
                        new MatchAllDocsQuery(),
                        request,
                        keywordField("i"),
                        keywordField("j"),
                        keywordField("k")
                    );
                    assertThat(result.getSubsetSize(), equalTo(1000L));
                    for (int i = 0; i < 10; i++) {
                        SignificantStringTerms.Bucket iBucket = result.getBucketByKey(Integer.toString(i));
                        assertThat(iBucket.getDocCount(), equalTo(100L));
                        SignificantStringTerms jAgg = iBucket.getAggregations().get("j");
                        assertThat(jAgg.getSubsetSize(), equalTo(100L));
                        for (int j = 0; j < 10; j++) {
                            SignificantStringTerms.Bucket jBucket = jAgg.getBucketByKey(Integer.toString(j));
                            assertThat(jBucket.getDocCount(), equalTo(10L));
                            SignificantStringTerms kAgg = jBucket.getAggregations().get("k");
                            assertThat(kAgg.getSubsetSize(), equalTo(10L));
                            for (int k = 0; k < 10; k++) {
                                SignificantStringTerms.Bucket kBucket = kAgg.getBucketByKey(Integer.toString(k));
                                assertThat(jAgg.getSubsetSize(), equalTo(100L));
                                assertThat(kBucket.getDocCount(), equalTo(1L));
                            }
                        }
                    }
                }
            }
        }
    }

    public void testThreeLayerLong() throws IOException {
        try (Directory dir = newDirectory()) {
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
                for (int i = 0; i < 10; i++) {
                    for (int j = 0; j < 10; j++) {
                        for (int k = 0; k < 10; k++) {
                            Document d = new Document();
                            d.add(new SortedNumericDocValuesField("i", i));
                            d.add(new SortedNumericDocValuesField("j", j));
                            d.add(new SortedNumericDocValuesField("k", k));
                            writer.addDocument(d);
                        }
                    }
                }
                try (IndexReader reader = maybeWrapReaderEs(writer.getReader())) {
                    IndexSearcher searcher = newIndexSearcher(reader);
                    SignificantTermsAggregationBuilder request = new SignificantTermsAggregationBuilder("i").field("i")
                        .minDocCount(0)
                        .subAggregation(
                            new SignificantTermsAggregationBuilder("j").field("j")
                                .minDocCount(0)
                                .subAggregation(new SignificantTermsAggregationBuilder("k").field("k").minDocCount(0))
                        );
                    SignificantLongTerms result = searchAndReduce(
                        searcher,
                        new MatchAllDocsQuery(),
                        request,
                        longField("i"),
                        longField("j"),
                        longField("k")
                    );
                    assertThat(result.getSubsetSize(), equalTo(1000L));
                    for (int i = 0; i < 10; i++) {
                        SignificantLongTerms.Bucket iBucket = result.getBucketByKey(Integer.toString(i));
                        assertThat(iBucket.getDocCount(), equalTo(100L));
                        SignificantLongTerms jAgg = iBucket.getAggregations().get("j");
                        assertThat(jAgg.getSubsetSize(), equalTo(100L));
                        for (int j = 0; j < 10; j++) {
                            SignificantLongTerms.Bucket jBucket = jAgg.getBucketByKey(Integer.toString(j));
                            assertThat(jBucket.getDocCount(), equalTo(10L));
                            SignificantLongTerms kAgg = jBucket.getAggregations().get("k");
                            assertThat(kAgg.getSubsetSize(), equalTo(10L));
                            for (int k = 0; k < 10; k++) {
                                SignificantLongTerms.Bucket kBucket = kAgg.getBucketByKey(Integer.toString(k));
                                assertThat(kBucket.getDocCount(), equalTo(1L));
                            }
                        }
                    }
                }
            }
        }
    }

    private void addMixedTextDocs(IndexWriter w) throws IOException {
        for (int i = 0; i < 10; i++) {
            Document doc = new Document();
            StringBuilder text = new StringBuilder("common ");
            if (i % 2 == 0) {
                text.append("odd ");
            } else {
                text.append("even ");
            }

            doc.add(new Field("text", text.toString(), TextFieldMapper.Defaults.FIELD_TYPE));
            String json = "{ \"text\" : \"" + text.toString() + "\" }";
            doc.add(new StoredField("_source", new BytesRef(json)));

            w.addDocument(doc);
        }
    }

    private void addFields(Document doc, List<Field> createFields) {
        for (Field field : createFields) {
            doc.add(field);
        }
    }

    public String randomExecutionHint() {
        return randomBoolean() ? null : randomFrom(ExecutionMode.values()).toString();
    }

}
