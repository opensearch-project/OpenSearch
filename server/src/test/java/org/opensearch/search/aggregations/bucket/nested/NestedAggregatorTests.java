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

package org.opensearch.search.aggregations.bucket.nested;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.support.NestedScope;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.InternalTerms;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalSum;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.SumAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.InternalSimpleValue;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.ValueType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import org.mockito.Mockito;

import static java.util.stream.Collectors.toList;
import static org.opensearch.search.aggregations.AggregationBuilders.max;
import static org.opensearch.search.aggregations.AggregationBuilders.nested;
import static org.opensearch.search.aggregations.bucket.nested.NestedAggregator.getParentAndChildId;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NestedAggregatorTests extends AggregatorTestCase {

    private static final String VALUE_FIELD_NAME = "number";
    private static final String VALUE_FIELD_NAME2 = "number2";
    private static final String NESTED_OBJECT = "nested_object";
    private static final String NESTED_OBJECT2 = "nested_object2";
    private static final String NESTED_AGG = "nestedAgg";
    private static final String MAX_AGG_NAME = "maxAgg";
    private static final String SUM_AGG_NAME = "sumAgg";
    private static final String INVERSE_SCRIPT = "inverse";
    private static final String OUT_NESTED = "outNested";
    private static final String OUT_TERMS = "outTerms";
    private static final String INNER_NESTED = "innerNested";
    private static final String INNER_TERMS = "innerTerms";

    private static final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();

    /**
     * For each provided field type, we also register an alias with name {@code <field>-alias}.
     */
    @Override
    protected Map<String, MappedFieldType> getFieldAliases(MappedFieldType... fieldTypes) {
        return Arrays.stream(fieldTypes).collect(Collectors.toMap(ft -> ft.name() + "-alias", Function.identity()));
    }

    /**
     * Nested aggregations need the {@linkplain DirectoryReader} wrapped.
     */
    @Override
    protected IndexReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return wrapInMockESDirectoryReader(reader);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        scripts.put(INVERSE_SCRIPT, vars -> -((Number) vars.get("_value")).doubleValue());
        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    public void testNoDocs() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                // intentionally not writing any docs
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    nestedBuilder,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(0, nested.getDocCount());

                InternalMax max = (InternalMax) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(Double.NEGATIVE_INFINITY, max.getValue(), Double.MIN_VALUE);
                assertFalse(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testSingleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(
                        expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME)
                    );
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(NESTED_OBJECT + "." + VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NESTED_OBJECT + "." + VALUE_FIELD_NAME,
                    NumberFieldMapper.NumberType.LONG
                );

                InternalNested nested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    nestedBuilder,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalMax max = (InternalMax) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);

                if (expectedNestedDocs > 0) {
                    assertTrue(AggregationInspectionHelper.hasValue(nested));
                } else {
                    assertFalse(AggregationInspectionHelper.hasValue(nested));
                }
            }
        }
    }

    public void testDoubleNestingMax() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedMaxValue = Math.max(
                        expectedMaxValue,
                        generateMaxDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME)
                    );
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder(MAX_AGG_NAME).field(NESTED_OBJECT + "." + VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(maxAgg);

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NESTED_OBJECT + "." + VALUE_FIELD_NAME,
                    NumberFieldMapper.NumberType.LONG
                );

                InternalNested nested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    nestedBuilder,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalMax max = (InternalMax) nested.getProperty(MAX_AGG_NAME);
                assertEquals(MAX_AGG_NAME, max.getName());
                assertEquals(expectedMaxValue, max.getValue(), Double.MIN_VALUE);

                if (expectedNestedDocs > 0) {
                    assertTrue(AggregationInspectionHelper.hasValue(nested));
                } else {
                    assertFalse(AggregationInspectionHelper.hasValue(nested));
                }
            }
        }
    }

    public void testOrphanedDocs() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedSum = 0;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedSum += generateSumDocs(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME);
                    expectedNestedDocs += numNestedDocs;

                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                // add some random nested docs that don't belong
                List<Document> documents = new ArrayList<>();
                int numOrphanedDocs = randomIntBetween(0, 20);
                generateSumDocs(documents, numOrphanedDocs, 1234, "foo", VALUE_FIELD_NAME);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT);
                SumAggregationBuilder sumAgg = new SumAggregationBuilder(SUM_AGG_NAME).field(NESTED_OBJECT + "." + VALUE_FIELD_NAME);
                nestedBuilder.subAggregation(sumAgg);
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NESTED_OBJECT + "." + VALUE_FIELD_NAME,
                    NumberFieldMapper.NumberType.LONG
                );

                InternalNested nested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    nestedBuilder,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalSum sum = (InternalSum) ((InternalAggregation) nested).getProperty(SUM_AGG_NAME);
                assertEquals(SUM_AGG_NAME, sum.getName());
                assertEquals(expectedSum, sum.getValue(), Double.MIN_VALUE);
            }
        }
    }

    public void testResetRootDocId() throws Exception {
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory, iwc)) {
                List<Document> documents = new ArrayList<>();

                // 1 segment with, 1 root document, with 3 nested sub docs
                Document document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();

                documents.clear();
                // 1 segment with:
                // 1 document, with 1 nested subdoc
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("2"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("2"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                documents.clear();
                // and 1 document, with 1 nested subdoc
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("3"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("3"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);

                iw.commit();
                iw.close();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {

                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, "nested_field");
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                BooleanQuery.Builder bq = new BooleanQuery.Builder();
                bq.add(Queries.newNonNestedFilter(), BooleanClause.Occur.MUST);
                bq.add(new TermQuery(new Term(IdFieldMapper.NAME, Uid.encodeId("2"))), BooleanClause.Occur.MUST_NOT);

                InternalNested nested = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    new ConstantScoreQuery(bq.build()),
                    nestedBuilder,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                // The bug manifests if 6 docs are returned, because currentRootDoc isn't reset the previous child docs from the first
                // segment are emitted as hits.
                assertEquals(4L, nested.getDocCount());

                assertTrue(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testNestedOrdering() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                iw.addDocuments(generateBook("1", new String[] { "a" }, new int[] { 12, 13, 14 }));
                iw.addDocuments(generateBook("2", new String[] { "b" }, new int[] { 5, 50 }));
                iw.addDocuments(generateBook("3", new String[] { "c" }, new int[] { 39, 19 }));
                iw.addDocuments(generateBook("4", new String[] { "d" }, new int[] { 2, 1, 3 }));
                iw.addDocuments(generateBook("5", new String[] { "a" }, new int[] { 70, 10 }));
                iw.addDocuments(generateBook("6", new String[] { "e" }, new int[] { 23, 21 }));
                iw.addDocuments(generateBook("7", new String[] { "e", "a" }, new int[] { 8, 8 }));
                iw.addDocuments(generateBook("8", new String[] { "f" }, new int[] { 12, 14 }));
                iw.addDocuments(generateBook("9", new String[] { "g", "c", "e" }, new int[] { 18, 8 }));
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("num_pages", NumberFieldMapper.NumberType.LONG);
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .field("author")
                    .order(BucketOrder.aggregation("chapters>num_pages.value", true));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MaxAggregationBuilder maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                Terms terms = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    termsBuilder,
                    fieldType1,
                    fieldType2
                );

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                Terms.Bucket bucket = terms.getBuckets().get(0);
                assertEquals("d", bucket.getKeyAsString());
                Max numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.getValue());

                bucket = terms.getBuckets().get(1);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.getValue());

                bucket = terms.getBuckets().get(2);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.getValue());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.getValue());

                bucket = terms.getBuckets().get(4);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.getValue());

                bucket = terms.getBuckets().get(5);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.getValue());

                bucket = terms.getBuckets().get(6);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.getValue());

                // reverse order:
                termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .field("author")
                    .order(BucketOrder.aggregation("chapters>num_pages.value", false));
                nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                maxAgg = new MaxAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(maxAgg);
                termsBuilder.subAggregation(nestedBuilder);

                terms = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    termsBuilder,
                    fieldType1,
                    fieldType2
                );

                assertEquals(7, terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                bucket = terms.getBuckets().get(0);
                assertEquals("a", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(70, (int) numPages.getValue());

                bucket = terms.getBuckets().get(1);
                assertEquals("b", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(50, (int) numPages.getValue());

                bucket = terms.getBuckets().get(2);
                assertEquals("c", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(39, (int) numPages.getValue());

                bucket = terms.getBuckets().get(3);
                assertEquals("e", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(23, (int) numPages.getValue());

                bucket = terms.getBuckets().get(4);
                assertEquals("g", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(18, (int) numPages.getValue());

                bucket = terms.getBuckets().get(5);
                assertEquals("f", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(14, (int) numPages.getValue());

                bucket = terms.getBuckets().get(6);
                assertEquals("d", bucket.getKeyAsString());
                numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                assertEquals(3, (int) numPages.getValue());
            }
        }
    }

    public void testNestedOrdering_random() throws IOException {
        int numBooks = randomIntBetween(32, 512);
        List<Tuple<String, int[]>> books = new ArrayList<>();
        for (int i = 0; i < numBooks; i++) {
            int numChapters = randomIntBetween(1, 8);
            int[] chapters = new int[numChapters];
            for (int j = 0; j < numChapters; j++) {
                chapters[j] = randomIntBetween(2, 64);
            }
            books.add(Tuple.tuple(String.format(Locale.ROOT, "%03d", i), chapters));
        }
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                int id = 0;
                for (Tuple<String, int[]> book : books) {
                    iw.addDocuments(generateBook(String.format(Locale.ROOT, "%03d", id), new String[] { book.v1() }, book.v2()));
                    id++;
                }
            }
            for (Tuple<String, int[]> book : books) {
                Arrays.sort(book.v2());
            }
            books.sort((o1, o2) -> {
                int cmp = Integer.compare(o1.v2()[0], o2.v2()[0]);
                if (cmp == 0) {
                    return o1.v1().compareTo(o2.v1());
                } else {
                    return cmp;
                }
            });
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType("num_pages", NumberFieldMapper.NumberType.LONG);
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("author");

                TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("authors").userValueTypeHint(ValueType.STRING)
                    .size(books.size())
                    .field("author")
                    .order(BucketOrder.compound(BucketOrder.aggregation("chapters>num_pages.value", true), BucketOrder.key(true)));
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder("chapters", "nested_chapters");
                MinAggregationBuilder minAgg = new MinAggregationBuilder("num_pages").field("num_pages");
                nestedBuilder.subAggregation(minAgg);
                termsBuilder.subAggregation(nestedBuilder);

                Terms terms = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    termsBuilder,
                    fieldType1,
                    fieldType2
                );

                assertEquals(books.size(), terms.getBuckets().size());
                assertEquals("authors", terms.getName());

                for (int i = 0; i < books.size(); i++) {
                    Tuple<String, int[]> book = books.get(i);
                    Terms.Bucket bucket = terms.getBuckets().get(i);
                    assertEquals(book.v1(), bucket.getKeyAsString());
                    Min numPages = ((Nested) bucket.getAggregations().get("chapters")).getAggregations().get("num_pages");
                    assertEquals(book.v2()[0], (int) numPages.getValue());
                }
            }
        }
    }

    public void testPreGetChildLeafCollectors() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                List<Document> documents = new ArrayList<>();
                Document document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a1")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b1")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("1"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("2"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a2")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("2"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b2")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("2"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
                documents.clear();

                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("3"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key1")));
                document.add(new SortedDocValuesField("value", new BytesRef("a3")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("3"), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(new SortedDocValuesField("key", new BytesRef("key2")));
                document.add(new SortedDocValuesField("value", new BytesRef("b3")));
                documents.add(document);
                document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId("3"), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                documents.add(document);
                iw.addDocuments(documents);
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                TermsAggregationBuilder valueBuilder = new TermsAggregationBuilder("value").userValueTypeHint(ValueType.STRING)
                    .field("value");
                TermsAggregationBuilder keyBuilder = new TermsAggregationBuilder("key").userValueTypeHint(ValueType.STRING).field("key");
                keyBuilder.subAggregation(valueBuilder);
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, "nested_field");
                nestedBuilder.subAggregation(keyBuilder);
                FilterAggregationBuilder filterAggregationBuilder = new FilterAggregationBuilder("filterAgg", new MatchAllQueryBuilder());
                filterAggregationBuilder.subAggregation(nestedBuilder);

                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("key");
                MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("value");

                Filter filter = searchAndReduce(
                    newSearcher(indexReader, false, true),
                    Queries.newNonNestedFilter(),
                    filterAggregationBuilder,
                    fieldType1,
                    fieldType2
                );

                assertEquals("filterAgg", filter.getName());
                assertEquals(3L, filter.getDocCount());

                InternalNested nested = filter.getAggregations().get(NESTED_AGG);
                assertEquals(6L, nested.getDocCount());

                StringTerms keyAgg = nested.getAggregations().get("key");
                assertEquals(2, keyAgg.getBuckets().size());
                Terms.Bucket key1 = keyAgg.getBuckets().get(0);
                assertEquals("key1", key1.getKey());
                StringTerms valueAgg = key1.getAggregations().get("value");
                assertEquals(3, valueAgg.getBuckets().size());
                assertEquals("a1", valueAgg.getBuckets().get(0).getKey());
                assertEquals("a2", valueAgg.getBuckets().get(1).getKey());
                assertEquals("a3", valueAgg.getBuckets().get(2).getKey());

                Terms.Bucket key2 = keyAgg.getBuckets().get(1);
                assertEquals("key2", key2.getKey());
                valueAgg = key2.getAggregations().get("value");
                assertEquals(3, valueAgg.getBuckets().size());
                assertEquals("b1", valueAgg.getBuckets().get(0).getKey());
                assertEquals("b2", valueAgg.getBuckets().get(1).getKey());
                assertEquals("b3", valueAgg.getBuckets().get(2).getKey());
            }
        }
    }

    public void testFieldAlias() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    int numNestedDocs = randomIntBetween(0, 20);
                    expectedNestedDocs += numNestedDocs;
                    generateDocuments(documents, numNestedDocs, i, NESTED_OBJECT, VALUE_FIELD_NAME);

                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }

            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder agg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(max(MAX_AGG_NAME).field(VALUE_FIELD_NAME));
                NestedAggregationBuilder aliasAgg = nested(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    max(MAX_AGG_NAME).field(VALUE_FIELD_NAME + "-alias")
                );

                InternalNested nested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    agg,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );
                Nested aliasNested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    aliasAgg,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );

                assertEquals(nested, aliasNested);
                assertEquals(expectedNestedDocs, nested.getDocCount());
            }
        }
    }

    /**
     * This tests to make sure pipeline aggs embedded under a SingleBucket agg (like nested)
     * are properly reduced
     */
    public void testNestedWithPipeline() throws IOException {
        int numRootDocs = randomIntBetween(1, 20);
        int expectedNestedDocs = 0;
        double expectedMaxValue = Double.NEGATIVE_INFINITY;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {
                    List<Document> documents = new ArrayList<>();
                    expectedMaxValue = Math.max(expectedMaxValue, generateMaxDocs(documents, 1, i, NESTED_OBJECT, VALUE_FIELD_NAME));
                    expectedNestedDocs += 1;

                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                NestedAggregationBuilder nestedBuilder = new NestedAggregationBuilder(NESTED_AGG, NESTED_OBJECT).subAggregation(
                    new TermsAggregationBuilder("terms").field(VALUE_FIELD_NAME)
                        .userValueTypeHint(ValueType.NUMERIC)
                        .subAggregation(new MaxAggregationBuilder(MAX_AGG_NAME).field(VALUE_FIELD_NAME))
                        .subAggregation(
                            new BucketScriptPipelineAggregationBuilder(
                                "bucketscript",
                                Collections.singletonMap("_value", MAX_AGG_NAME),
                                new Script(ScriptType.INLINE, MockScriptEngine.NAME, INVERSE_SCRIPT, Collections.emptyMap())
                            )
                        )
                );

                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(VALUE_FIELD_NAME, NumberFieldMapper.NumberType.LONG);

                InternalNested nested = searchAndReduce(
                    createIndexSettings(),
                    newSearcher(indexReader, false, true),
                    new MatchAllDocsQuery(),
                    nestedBuilder,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType
                );

                assertEquals(NESTED_AGG, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                InternalTerms<?, LongTerms.Bucket> terms = (InternalTerms) nested.getProperty("terms");
                assertNotNull(terms);

                for (LongTerms.Bucket bucket : terms.getBuckets()) {
                    InternalMax max = (InternalMax) bucket.getAggregations().asMap().get(MAX_AGG_NAME);
                    InternalSimpleValue bucketScript = (InternalSimpleValue) bucket.getAggregations().asMap().get("bucketscript");
                    assertNotNull(max);
                    assertNotNull(bucketScript);
                    assertEquals(max.getValue(), -bucketScript.getValue(), Double.MIN_VALUE);
                }

                assertTrue(AggregationInspectionHelper.hasValue(nested));
            }
        }
    }

    public void testNestedUnderTerms() throws IOException {
        int numProducts = scaledRandomIntBetween(1, 100);
        int numResellers = scaledRandomIntBetween(1, 100);

        AggregationBuilder b = new TermsAggregationBuilder("products").field("product_id")
            .size(numProducts)
            .subAggregation(
                new NestedAggregationBuilder("nested", "nested_reseller").subAggregation(
                    new TermsAggregationBuilder("resellers").field("reseller_id").size(numResellers)
                )
            );
        testCase(b, new MatchAllDocsQuery(), buildResellerData(numProducts, numResellers), result -> {
            LongTerms products = (LongTerms) result;
            assertThat(
                products.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                equalTo(LongStream.range(0, numProducts).mapToObj(Long::valueOf).collect(toList()))
            );
            for (int p = 0; p < numProducts; p++) {
                LongTerms.Bucket bucket = products.getBucketByKey(Integer.toString(p));
                assertThat(bucket.getDocCount(), equalTo(1L));
                InternalNested nested = bucket.getAggregations().get("nested");
                assertThat(nested.getDocCount(), equalTo((long) numResellers));
                LongTerms resellers = nested.getAggregations().get("resellers");
                assertThat(
                    resellers.getBuckets().stream().map(LongTerms.Bucket::getKeyAsNumber).collect(toList()),
                    equalTo(LongStream.range(0, numResellers).mapToObj(Long::valueOf).collect(toList()))
                );
            }
        }, resellersMappedFields());
    }

    public void testBufferingNestedLeafBucketCollector() throws IOException {
        int numRootDocs = scaledRandomIntBetween(2, 200);
        int expectedNestedDocs;
        String[] bucketKeys;
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < numRootDocs; i++) {

                    List<Document> documents = new ArrayList<>();
                    if (randomBoolean()) {
                        generateDocument(documents, i, NESTED_OBJECT, VALUE_FIELD_NAME, 1);
                        generateDocument(documents, i, NESTED_OBJECT2, VALUE_FIELD_NAME2, i);
                    } else {
                        generateDocument(documents, i, NESTED_OBJECT2, VALUE_FIELD_NAME2, i);
                        generateDocument(documents, i, NESTED_OBJECT, VALUE_FIELD_NAME, 1);
                    }
                    Document document = new Document();
                    document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(i)), IdFieldMapper.Defaults.FIELD_TYPE));
                    document.add(sequenceIDFields.primaryTerm);
                    documents.add(document);
                    iw.addDocuments(documents);
                }
                iw.commit();
            }
            try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                IndexSettings indexSettings = createIndexSettings();
                MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                    NESTED_OBJECT + "." + VALUE_FIELD_NAME,
                    NumberFieldMapper.NumberType.LONG
                );
                MappedFieldType fieldType1 = new NumberFieldMapper.NumberFieldType(
                    NESTED_OBJECT2 + "." + VALUE_FIELD_NAME2,
                    NumberFieldMapper.NumberType.LONG
                );
                QueryShardContext queryShardContext = createQueryShardContext(NESTED_OBJECT2, indexSettings, fieldType1);
                // query
                expectedNestedDocs = numRootDocs / 2;
                bucketKeys = new String[expectedNestedDocs];
                BytesRef[] values = new BytesRef[numRootDocs / 2];
                for (int i = 0; i < numRootDocs / 2; i++) {
                    bucketKeys[i] = "" + (i * 2);
                    values[i] = new BytesRef(bucketKeys[i]);
                }
                TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder(NESTED_OBJECT2 + "." + VALUE_FIELD_NAME2, (Object[]) values);
                NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(NESTED_OBJECT2, termsQueryBuilder, ScoreMode.None);

                // out nested aggs
                NestedAggregationBuilder outNestedBuilder = new NestedAggregationBuilder(OUT_NESTED, NESTED_OBJECT);
                TermsAggregationBuilder outTermsAggregator = new TermsAggregationBuilder(OUT_TERMS).field(
                    NESTED_OBJECT + "." + VALUE_FIELD_NAME
                ).size(100);
                outNestedBuilder.subAggregation(outTermsAggregator);

                // inner nested aggs
                NestedAggregationBuilder innerNestedBuilder = new NestedAggregationBuilder(INNER_NESTED, NESTED_OBJECT2);
                TermsAggregationBuilder innerTermsAggregator = new TermsAggregationBuilder(INNER_TERMS).field(
                    NESTED_OBJECT2 + "." + VALUE_FIELD_NAME2
                ).size(100);
                innerNestedBuilder.subAggregation(innerTermsAggregator);
                outTermsAggregator.subAggregation(innerNestedBuilder);

                InternalNested nested = searchAndReduce(
                    indexSettings,
                    newSearcher(indexReader, false, true),
                    nestedQueryBuilder.toQuery(queryShardContext),
                    outNestedBuilder,
                    DEFAULT_MAX_BUCKETS,
                    true,
                    fieldType,
                    fieldType1
                );

                assertEquals(OUT_NESTED, nested.getName());
                assertEquals(expectedNestedDocs, nested.getDocCount());

                LongTerms outTerms = (LongTerms) nested.getProperty(OUT_TERMS);
                assertEquals(1, outTerms.getBuckets().size());

                InternalNested internalNested = (InternalNested) (((Object[]) outTerms.getProperty(INNER_NESTED))[0]);
                assertEquals(expectedNestedDocs, internalNested.getDocCount());

                LongTerms innerTerms = (LongTerms) internalNested.getProperty(INNER_TERMS);
                assertEquals(bucketKeys.length, innerTerms.getBuckets().size());
                for (int i = 0; i < expectedNestedDocs; i++) {
                    LongTerms.Bucket bucket = innerTerms.getBuckets().get(i);
                    assertEquals(bucketKeys[i], bucket.getKeyAsString());
                    assertEquals(1, bucket.getDocCount());
                }
            }
        }
    }

    private DocIdSetIterator getDocIdSetIterator(int[] value) {
        int[] bits = new int[value[value.length - 1] + 1];
        for (int i : value) {
            bits[i] = 1;
        }
        return new DocIdSetIterator() {
            int index = -1;

            @Override
            public int docID() {
                if (index == -1 || index > bits.length || bits[index] != 1) {
                    return -1;
                }
                return index;
            }

            @Override
            public int nextDoc() {
                for (int i = index; i < bits.length; i++) {
                    if (bits[i] == 1) {
                        index = i;
                        return index;
                    }
                }
                index = bits.length;
                return NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                for (int i = target; i < bits.length; i++) {
                    if (bits[i] == 1) {
                        index = i;
                        return index;
                    }
                }
                index = bits.length;
                return NO_MORE_DOCS;
            }

            @Override
            public long cost() {
                return bits.length;
            }
        };
    }

    public void testGetParentAndChildId() throws IOException {
        {
            // p: parent c: child
            // [p0], [p1], [c2,p3], [c4,x5,p6], [p7], [p8]
            BitSet parentDocs = new FixedBitSet(20);
            parentDocs.set(0);
            parentDocs.set(1);
            parentDocs.set(3);
            parentDocs.set(6);
            parentDocs.set(7);
            parentDocs.set(8);
            DocIdSetIterator childDocs = getDocIdSetIterator(new int[] { 2, 4 });

            Tuple<Integer, Integer> res = getParentAndChildId(parentDocs, childDocs, 0);
            assertEquals(0, res.v1().intValue());
            assertEquals(2, res.v2().intValue());

            res = getParentAndChildId(parentDocs, childDocs, 3);
            assertEquals(3, res.v1().intValue());
            assertEquals(2, res.v2().intValue());

            res = getParentAndChildId(parentDocs, childDocs, 4);
            assertEquals(6, res.v1().intValue());
            assertEquals(4, res.v2().intValue());

            res = getParentAndChildId(parentDocs, childDocs, 8);
            assertEquals(8, res.v1().intValue());
            assertEquals(NO_MORE_DOCS, res.v2().intValue());
        }

        {
            // p: parent c: child1 d: child2
            // [p0], [c1,d2,p3], [d4,c5,p6], [c7,d8,p9], [c10,p11]
            BitSet parentDocs = new FixedBitSet(20);
            parentDocs.set(0);
            parentDocs.set(3);
            parentDocs.set(6);
            parentDocs.set(9);
            parentDocs.set(11);
            {
                DocIdSetIterator childDocs = getDocIdSetIterator(new int[] { 1, 5, 7, 10 });
                Tuple<Integer, Integer> res = getParentAndChildId(parentDocs, childDocs, 2);
                assertEquals(3, res.v1().intValue());
                assertEquals(1, res.v2().intValue());

                res = getParentAndChildId(parentDocs, childDocs, 4);
                assertEquals(6, res.v1().intValue());
                assertEquals(5, res.v2().intValue());

                res = getParentAndChildId(parentDocs, childDocs, 8);
                assertEquals(9, res.v1().intValue());
                assertEquals(7, res.v2().intValue());
            }

            {
                DocIdSetIterator childDocs = getDocIdSetIterator(new int[] { 2, 4, 8 });
                Tuple<Integer, Integer> res = getParentAndChildId(parentDocs, childDocs, 1);
                assertEquals(3, res.v1().intValue());
                assertEquals(2, res.v2().intValue());

                res = getParentAndChildId(parentDocs, childDocs, 5);
                assertEquals(6, res.v1().intValue());
                assertEquals(4, res.v2().intValue());

                res = getParentAndChildId(parentDocs, childDocs, 7);
                assertEquals(9, res.v1().intValue());
                assertEquals(8, res.v2().intValue());

                res = getParentAndChildId(parentDocs, childDocs, 10);
                assertEquals(11, res.v1().intValue());
                assertEquals(NO_MORE_DOCS, res.v2().intValue());
            }
        }
    }

    protected QueryShardContext createQueryShardContext(String fieldName, IndexSettings indexSettings, MappedFieldType fieldType) {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.nestedScope()).thenReturn(new NestedScope(indexSettings));

        BitsetFilterCache bitsetFilterCache = new BitsetFilterCache(indexSettings, Mockito.mock(BitsetFilterCache.Listener.class));
        when(queryShardContext.bitsetFilter(any())).thenReturn(bitsetFilterCache.getBitSetProducer(Queries.newNonNestedFilter()));
        when(queryShardContext.fieldMapper(anyString())).thenReturn(fieldType);
        when(queryShardContext.getSearchQuoteAnalyzer(any())).thenCallRealMethod();
        when(queryShardContext.getSearchAnalyzer(any())).thenCallRealMethod();
        when(queryShardContext.getIndexSettings()).thenReturn(indexSettings);
        when(queryShardContext.getObjectMapper(anyString())).thenAnswer(invocation -> {
            Mapper.BuilderContext context = new Mapper.BuilderContext(indexSettings.getSettings(), new ContentPath());
            return new ObjectMapper.Builder<>(fieldName).nested(ObjectMapper.Nested.newNested()).build(context);
        });
        when(queryShardContext.allowExpensiveQueries()).thenReturn(true);
        return queryShardContext;
    }

    public static CheckedConsumer<RandomIndexWriter, IOException> buildResellerData(int numProducts, int numResellers) {
        return iw -> {
            for (int p = 0; p < numProducts; p++) {
                List<Document> documents = new ArrayList<>();
                generateDocuments(documents, numResellers, p, "nested_reseller", "value");
                for (int r = 0; r < documents.size(); r++) {
                    documents.get(r).add(new SortedNumericDocValuesField("reseller_id", r));
                }
                Document document = new Document();
                document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(p)), IdFieldMapper.Defaults.FIELD_TYPE));
                document.add(new Field(NestedPathFieldMapper.NAME, "nested_field", NestedPathFieldMapper.Defaults.FIELD_TYPE));
                document.add(sequenceIDFields.primaryTerm);
                document.add(new SortedNumericDocValuesField("product_id", p));
                documents.add(document);
                iw.addDocuments(documents);
            }
        };
    }

    public static MappedFieldType[] resellersMappedFields() {
        MappedFieldType productIdField = new NumberFieldMapper.NumberFieldType("product_id", NumberFieldMapper.NumberType.LONG);
        MappedFieldType resellerIdField = new NumberFieldMapper.NumberFieldType("reseller_id", NumberFieldMapper.NumberType.LONG);
        return new MappedFieldType[] { productIdField, resellerIdField };
    }

    private double generateMaxDocs(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName)).max().orElse(Double.NEGATIVE_INFINITY);
    }

    private double generateSumDocs(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {
        return DoubleStream.of(generateDocuments(documents, numNestedDocs, id, path, fieldName)).sum();
    }

    private static double[] generateDocuments(List<Document> documents, int numNestedDocs, int id, String path, String fieldName) {
        double[] values = new double[numNestedDocs];
        for (int nested = 0; nested < numNestedDocs; nested++) {
            Document document = new Document();
            document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(id)), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(NestedPathFieldMapper.NAME, path, NestedPathFieldMapper.Defaults.FIELD_TYPE));
            long value = randomNonNegativeLong() % 10000;
            document.add(new SortedNumericDocValuesField(path + "." + fieldName, value));
            documents.add(document);
            values[nested] = value;
        }
        return values;
    }

    private static void generateDocument(List<Document> documents, int id, String path, String fieldName, long vales) {
        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(Integer.toString(id)), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
        document.add(new Field(NestedPathFieldMapper.NAME, path, NestedPathFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedNumericDocValuesField(path + "." + fieldName, vales));
        document.add(new LongPoint(path + "." + fieldName, vales));
        documents.add(document);
    }

    private List<Document> generateBook(String id, String[] authors, int[] numPages) {
        List<Document> documents = new ArrayList<>();

        for (int numPage : numPages) {
            Document document = new Document();
            document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(NestedPathFieldMapper.NAME, "nested_chapters", NestedPathFieldMapper.Defaults.FIELD_TYPE));
            document.add(new SortedNumericDocValuesField("num_pages", numPage));
            documents.add(document);
        }

        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        document.add(sequenceIDFields.primaryTerm);
        for (String author : authors) {
            document.add(new SortedSetDocValuesField("author", new BytesRef(author)));
        }
        documents.add(document);

        return documents;
    }

}
