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

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.TriConsumer;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.InternalFilter;
import org.opensearch.search.aggregations.bucket.global.GlobalAggregationBuilder;
import org.opensearch.search.aggregations.bucket.global.InternalGlobal;
import org.opensearch.search.aggregations.bucket.nested.InternalNested;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalTopHits;
import org.opensearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketScriptPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.test.geo.RandomGeoGenerator;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.opensearch.index.mapper.SeqNoFieldMapper.PRIMARY_TERM_NAME;
import static org.opensearch.search.aggregations.AggregationBuilders.terms;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.bucketScript;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TermsAggregatorTests extends AggregatorTestCase {
    // Constants for a script that returns a string
    private static final String STRING_SCRIPT_NAME = "string_script";
    private static final String STRING_SCRIPT_OUTPUT = "Orange";

    @Override
    protected MapperService mapperServiceMock() {
        MapperService mapperService = mock(MapperService.class);
        DocumentMapper mapper = mock(DocumentMapper.class);
        when(mapper.typeText()).thenReturn(new Text("_doc"));
        when(mapper.type()).thenReturn("_doc");
        when(mapperService.documentMapper()).thenReturn(mapper);
        return mapperService;
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
        Map<String, Function<Map<String, Object>, Object>> nonDeterministicScripts = new HashMap<>();

        scripts.put(STRING_SCRIPT_NAME, value -> STRING_SCRIPT_OUTPUT);

        MockScriptEngine scriptEngine = new MockScriptEngine(
            MockScriptEngine.NAME,
            scripts,
            nonDeterministicScripts,
            Collections.emptyMap()
        );
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }

    protected CountingAggregator createCountingAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        boolean randomizeAggregatorImpl,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return new CountingAggregator(
            new AtomicInteger(),
            createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldTypes)
        );
    }

    protected <A extends Aggregator> A createAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        boolean randomizeAggregatorImpl,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try {
            if (randomizeAggregatorImpl) {
                TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = randomBoolean();
                TermsAggregatorFactory.REMAP_GLOBAL_ORDS = randomBoolean();
            }
            return super.createAggregator(aggregationBuilder, indexSearcher, fieldTypes);
        } finally {
            TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = null;
            TermsAggregatorFactory.REMAP_GLOBAL_ORDS = null;
        }
    }

    protected <A extends Aggregator> A createAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(aggregationBuilder, indexSearcher, true, fieldTypes);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new TermsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return Collections.unmodifiableList(
            asList(
                CoreValuesSourceType.NUMERIC,
                CoreValuesSourceType.BYTES,
                CoreValuesSourceType.IP,
                CoreValuesSourceType.DATE,
                CoreValuesSourceType.BOOLEAN
            )
        );
    }

    public void testUsesGlobalOrdinalsByDefault() throws Exception {
        boolean randomizeAggregatorImpl = false;
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        indexWriter.close();
        IndexReader indexReader = DirectoryReader.open(directory);
        // We do not use LuceneTestCase.newSearcher because we need a DirectoryReader
        IndexSearcher indexSearcher = new IndexSearcher(indexReader);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
            .field("string");
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("string");

        TermsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldType);
        assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.class));
        GlobalOrdinalsStringTermsAggregator globalAgg = (GlobalOrdinalsStringTermsAggregator) aggregator;
        assertThat(globalAgg.descriptCollectionStrategy(), equalTo("dense"));

        // Infers depth_first because the maxOrd is 0 which is less than the size
        aggregationBuilder.subAggregation(AggregationBuilders.cardinality("card").field("string"));
        aggregator = createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldType);
        assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.class));
        globalAgg = (GlobalOrdinalsStringTermsAggregator) aggregator;
        assertThat(globalAgg.collectMode, equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(globalAgg.descriptCollectionStrategy(), equalTo("remap"));

        aggregationBuilder.collectMode(Aggregator.SubAggCollectionMode.DEPTH_FIRST);
        aggregator = createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldType);
        assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.class));
        globalAgg = (GlobalOrdinalsStringTermsAggregator) aggregator;
        assertThat(globalAgg.collectMode, equalTo(Aggregator.SubAggCollectionMode.DEPTH_FIRST));
        assertThat(globalAgg.descriptCollectionStrategy(), equalTo("remap"));

        aggregationBuilder.collectMode(Aggregator.SubAggCollectionMode.BREADTH_FIRST);
        aggregator = createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldType);
        assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.class));
        globalAgg = (GlobalOrdinalsStringTermsAggregator) aggregator;
        assertThat(globalAgg.collectMode, equalTo(Aggregator.SubAggCollectionMode.BREADTH_FIRST));
        assertThat(globalAgg.descriptCollectionStrategy(), equalTo("dense"));

        aggregationBuilder.order(BucketOrder.aggregation("card", true));
        aggregator = createAggregator(aggregationBuilder, indexSearcher, randomizeAggregatorImpl, fieldType);
        assertThat(aggregator, instanceOf(GlobalOrdinalsStringTermsAggregator.class));
        globalAgg = (GlobalOrdinalsStringTermsAggregator) aggregator;
        assertThat(globalAgg.descriptCollectionStrategy(), equalTo("remap"));

        indexReader.close();
        directory.close();
    }

    /**
     * This test case utilizes the default implementation of GlobalOrdinalsStringTermsAggregator since collectSegmentOrds is false
     */
    public void testSimpleAggregation() throws Exception {
        // Fields not indexed: cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_NOT_INDEXED, false, false, false, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);

        // Fields indexed, deleted documents in segment: cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, true, false, false, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);

        // Fields indexed, no deleted documents in segment: will use LeafBucketCollector#termDocFreqCollector - no documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, false, false, false, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 0);

        // Fields indexed, no deleted documents, but _doc_field value present in document:
        // cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, false, true, false, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);

    }

    /**
     * This test case utilizes the LowCardinality implementation of GlobalOrdinalsStringTermsAggregator since collectSegmentOrds is true
     */
    public void testSimpleAggregationLowCardinality() throws Exception {
        // Fields not indexed: cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_NOT_INDEXED, false, false, true, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);

        // Fields indexed, deleted documents in segment: cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, true, false, true, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);

        // Fields indexed, no deleted documents in segment: will use LeafBucketCollector#termDocFreqCollector - no documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, false, false, true, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 0);

        // Fields indexed, no deleted documents, but _doc_field value present in document:
        // cannot use LeafBucketCollector#termDocFreqCollector - all documents are visited
        testSimple(ADD_SORTED_SET_FIELD_INDEXED, false, true, true, TermsAggregatorFactory.ExecutionMode.GLOBAL_ORDINALS, 4);
    }

    /**
     * This test case utilizes the MapStringTermsAggregator.
     */
    public void testSimpleMapStringAggregation() throws Exception {
        testSimple(
            ADD_SORTED_SET_FIELD_INDEXED,
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            TermsAggregatorFactory.ExecutionMode.MAP,
            4
        );
    }

    /**
     * This is a utility method to test out string terms aggregation
     * @param addFieldConsumer a function that determines how a field is added to the document
     * @param includeDeletedDocumentsInSegment to include deleted documents in the segment or not
     * @param collectSegmentOrds collect segment ords or not - set true to utilize LowCardinality implementation for GlobalOrdinalsStringTermsAggregator
     * @param executionMode execution mode MAP or GLOBAL_ORDINALS
     * @param expectedCollectCount expected number of documents visited as part of collect() invocation
     */
    private void testSimple(
        TriConsumer<Document, String, String> addFieldConsumer,
        final boolean includeDeletedDocumentsInSegment,
        final boolean includeDocCountField,
        boolean collectSegmentOrds,
        TermsAggregatorFactory.ExecutionMode executionMode,
        final int expectedCollectCount
    ) throws Exception {
        try (Directory directory = newDirectory()) {
            try (
                RandomIndexWriter indexWriter = new RandomIndexWriter(
                    random(),
                    directory,
                    newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE)
                )
            ) {
                List<Document> documents = new ArrayList<>();
                Document document = new Document();
                addFieldConsumer.apply(document, "string", "a");
                addFieldConsumer.apply(document, "string", "b");
                documents.add(document);

                document = new Document();
                addFieldConsumer.apply(document, "string", "");
                addFieldConsumer.apply(document, "string", "c");
                addFieldConsumer.apply(document, "string", "a");
                documents.add(document);

                document = new Document();
                addFieldConsumer.apply(document, "string", "b");
                addFieldConsumer.apply(document, "string", "d");
                documents.add(document);

                document = new Document();
                addFieldConsumer.apply(document, "string", "");
                if (includeDocCountField) {
                    // Adding _doc_count to one document
                    document.add(new NumericDocValuesField("_doc_count", 10));
                }
                documents.add(document);

                if (includeDeletedDocumentsInSegment) {
                    document = new Document();
                    ADD_SORTED_SET_FIELD_INDEXED.apply(document, "string", "e");
                    documents.add(document);
                    indexWriter.addDocuments(documents);
                    indexWriter.deleteDocuments(new Term("string", "e"));
                    assertEquals(5, indexWriter.getDocStats().maxDoc);  // deleted document still in segment
                } else {
                    indexWriter.addDocuments(documents);
                }

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionMode.toString())
                        .field("string")
                        .order(BucketOrder.key(true));
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("string");

                    TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = collectSegmentOrds;
                    TermsAggregatorFactory.REMAP_GLOBAL_ORDS = false;
                    CountingAggregator aggregator = createCountingAggregator(aggregationBuilder, indexSearcher, false, fieldType);

                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(5, result.getBuckets().size());
                    assertEquals("", result.getBuckets().get(0).getKeyAsString());
                    if (includeDocCountField) {
                        assertEquals(11L, result.getBuckets().get(0).getDocCount());
                    } else {
                        assertEquals(2L, result.getBuckets().get(0).getDocCount());
                    }
                    assertEquals("a", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(1).getDocCount());
                    assertEquals("b", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(2L, result.getBuckets().get(2).getDocCount());
                    assertEquals("c", result.getBuckets().get(3).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertEquals("d", result.getBuckets().get(4).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(4).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    assertEquals(expectedCollectCount, aggregator.getCollectCount().get());
                }
            }
        }
    }

    public void testStringIncludeExclude() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val000")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val001")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val001")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val002")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val003")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val003")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val004")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val005")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val005")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val006")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val007")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val007")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val008")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val009")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val009")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val010")));
                document.add(new SortedSetDocValuesField("mv_field", new BytesRef("val011")));
                document.add(new SortedDocValuesField("sv_field", new BytesRef("val011")));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("mv_field");

                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude("val00.+", null))
                        .field("mv_field")
                        .size(12)
                        .order(BucketOrder.key(true));

                    TermsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(10, result.getBuckets().size());
                    assertEquals("val000", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val001", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertEquals("val002", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals("val003", result.getBuckets().get(3).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertEquals("val004", result.getBuckets().get(4).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(4).getDocCount());
                    assertEquals("val005", result.getBuckets().get(5).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(5).getDocCount());
                    assertEquals("val006", result.getBuckets().get(6).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(6).getDocCount());
                    assertEquals("val007", result.getBuckets().get(7).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(7).getDocCount());
                    assertEquals("val008", result.getBuckets().get(8).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(8).getDocCount());
                    assertEquals("val009", result.getBuckets().get(9).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(9).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("sv_field");
                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude("val00.+", null))
                        .field("sv_field")
                        .order(BucketOrder.key(true));

                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(5, result.getBuckets().size());
                    assertEquals("val001", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val003", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertEquals("val005", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals("val007", result.getBuckets().get(3).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertEquals("val009", result.getBuckets().get(4).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(4).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude("val00.+", "(val000|val001)"))
                        .field("mv_field")
                        .order(BucketOrder.key(true));

                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(8, result.getBuckets().size());
                    assertEquals("val002", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val003", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertEquals("val004", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals("val005", result.getBuckets().get(3).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertEquals("val006", result.getBuckets().get(4).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(4).getDocCount());
                    assertEquals("val007", result.getBuckets().get(5).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(5).getDocCount());
                    assertEquals("val008", result.getBuckets().get(6).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(6).getDocCount());
                    assertEquals("val009", result.getBuckets().get(7).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(7).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(null, "val00.+"))
                        .field("mv_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(2, result.getBuckets().size());
                    assertEquals("val010", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val011", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(new String[] { "val000", "val010" }, null))
                        .field("mv_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(2, result.getBuckets().size());
                    assertEquals("val000", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val010", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .includeExclude(
                            new IncludeExclude(
                                null,
                                new String[] {
                                    "val001",
                                    "val002",
                                    "val003",
                                    "val004",
                                    "val005",
                                    "val006",
                                    "val007",
                                    "val008",
                                    "val009",
                                    "val011" }
                            )
                        )
                        .field("mv_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(2, result.getBuckets().size());
                    assertEquals("val000", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals("val010", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));
                }
            }
        }
    }

    public void testNumericIncludeExclude() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new NumericDocValuesField("long_field", 0));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(0.0)));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new NumericDocValuesField("long_field", 1));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(1.0)));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new NumericDocValuesField("long_field", 2));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(2.0)));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new NumericDocValuesField("long_field", 3));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(3.0)));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new NumericDocValuesField("long_field", 4));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(4.0)));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new NumericDocValuesField("long_field", 5));
                document.add(new NumericDocValuesField("double_field", Double.doubleToRawLongBits(5.0)));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("long_field", NumberFieldMapper.NumberType.LONG);

                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.LONG)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(new long[] { 0, 5 }, null))
                        .field("long_field")
                        .order(BucketOrder.key(true));
                    TermsAggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(2, result.getBuckets().size());
                    assertEquals(0L, result.getBuckets().get(0).getKey());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals(5L, result.getBuckets().get(1).getKey());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.LONG)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(null, new long[] { 0, 5 }))
                        .field("long_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(4, result.getBuckets().size());
                    assertEquals(1L, result.getBuckets().get(0).getKey());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals(2L, result.getBuckets().get(1).getKey());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertEquals(3L, result.getBuckets().get(2).getKey());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals(4L, result.getBuckets().get(3).getKey());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    fieldType = new NumberFieldMapper.NumberFieldType("double_field", NumberFieldMapper.NumberType.DOUBLE);
                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.DOUBLE)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(new double[] { 0.0, 5.0 }, null))
                        .field("double_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(2, result.getBuckets().size());
                    assertEquals(0.0, result.getBuckets().get(0).getKey());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals(5.0, result.getBuckets().get(1).getKey());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.DOUBLE)
                        .executionHint(executionHint)
                        .includeExclude(new IncludeExclude(null, new double[] { 0.0, 5.0 }))
                        .field("double_field")
                        .order(BucketOrder.key(true));
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals(4, result.getBuckets().size());
                    assertEquals(1.0, result.getBuckets().get(0).getKey());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    assertEquals(2.0, result.getBuckets().get(1).getKey());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    assertEquals(3.0, result.getBuckets().get(2).getKey());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    assertEquals(4.0, result.getBuckets().get(3).getKey());
                    assertEquals(1L, result.getBuckets().get(3).getDocCount());
                    assertTrue(AggregationInspectionHelper.hasValue((InternalTerms) result));
                }
            }
        }
    }

    public void testStringTermsAggregator() throws Exception {
        BiFunction<String, Boolean, IndexableField> luceneFieldFactory = (val, mv) -> {
            if (mv) {
                return new SortedSetDocValuesField("field", new BytesRef(val));
            } else {
                return new SortedDocValuesField("field", new BytesRef(val));
            }
        };
        MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");
        termsAggregator(ValueType.STRING, fieldType, i -> Integer.toString(i), String::compareTo, luceneFieldFactory);
        termsAggregatorWithNestedMaxAgg(
            ValueType.STRING,
            fieldType,
            i -> Integer.toString(i),
            val -> new SortedDocValuesField("field", new BytesRef(val))
        );
    }

    public void testLongTermsAggregator() throws Exception {
        BiFunction<Long, Boolean, IndexableField> luceneFieldFactory = (val, mv) -> {
            if (mv) {
                return new SortedNumericDocValuesField("field", val);
            } else {
                return new NumericDocValuesField("field", val);
            }
        };
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.LONG);
        termsAggregator(ValueType.LONG, fieldType, Integer::longValue, Long::compareTo, luceneFieldFactory);
        termsAggregatorWithNestedMaxAgg(ValueType.LONG, fieldType, Integer::longValue, val -> new NumericDocValuesField("field", val));
    }

    public void testDoubleTermsAggregator() throws Exception {
        BiFunction<Double, Boolean, IndexableField> luceneFieldFactory = (val, mv) -> {
            if (mv) {
                return new SortedNumericDocValuesField("field", Double.doubleToRawLongBits(val));
            } else {
                return new NumericDocValuesField("field", Double.doubleToRawLongBits(val));
            }
        };
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
        termsAggregator(ValueType.DOUBLE, fieldType, Integer::doubleValue, Double::compareTo, luceneFieldFactory);
        termsAggregatorWithNestedMaxAgg(
            ValueType.DOUBLE,
            fieldType,
            Integer::doubleValue,
            val -> new NumericDocValuesField("field", Double.doubleToRawLongBits(val))
        );
    }

    public void testIpTermsAggregator() throws Exception {
        BiFunction<InetAddress, Boolean, IndexableField> luceneFieldFactory = (val, mv) -> {
            if (mv) {
                return new SortedSetDocValuesField("field", new BytesRef(InetAddressPoint.encode(val)));
            } else {
                return new SortedDocValuesField("field", new BytesRef(InetAddressPoint.encode(val)));
            }
        };
        InetAddress[] base = new InetAddress[] { InetAddresses.forString("192.168.0.0") };
        Comparator<InetAddress> comparator = (o1, o2) -> {
            BytesRef b1 = new BytesRef(InetAddressPoint.encode(o1));
            BytesRef b2 = new BytesRef(InetAddressPoint.encode(o2));
            return b1.compareTo(b2);
        };
        termsAggregator(
            ValueType.IP,
            new IpFieldMapper.IpFieldType("field"),
            i -> base[0] = InetAddressPoint.nextUp(base[0]),
            comparator,
            luceneFieldFactory
        );
    }

    private <T> void termsAggregator(
        ValueType valueType,
        MappedFieldType fieldType,
        Function<Integer, T> valueFactory,
        Comparator<T> keyComparator,
        BiFunction<T, Boolean, IndexableField> luceneFieldFactory
    ) throws Exception {
        final Map<T, Integer> counts = new HashMap<>();
        final Map<T, Integer> filteredCounts = new HashMap<>();
        int numTerms = scaledRandomIntBetween(8, 128);
        for (int i = 0; i < numTerms; i++) {
            int numDocs = scaledRandomIntBetween(2, 32);
            T key = valueFactory.apply(i);
            counts.put(key, numDocs);
            filteredCounts.put(key, 0);
        }

        try (Directory directory = newDirectory()) {
            boolean multiValued = randomBoolean();
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                if (multiValued == false) {
                    for (Map.Entry<T, Integer> entry : counts.entrySet()) {
                        for (int i = 0; i < entry.getValue(); i++) {
                            Document document = new Document();
                            document.add(luceneFieldFactory.apply(entry.getKey(), false));
                            if (randomBoolean()) {
                                document.add(new StringField("include", "yes", Field.Store.NO));
                                filteredCounts.computeIfPresent(entry.getKey(), (key, integer) -> integer + 1);
                            }
                            indexWriter.addDocument(document);
                        }
                    }
                } else {
                    Iterator<Map.Entry<T, Integer>> iterator = counts.entrySet().iterator();
                    while (iterator.hasNext()) {
                        Map.Entry<T, Integer> entry1 = iterator.next();
                        Map.Entry<T, Integer> entry2 = null;
                        if (randomBoolean() && iterator.hasNext()) {
                            entry2 = iterator.next();
                            if (entry1.getValue().compareTo(entry2.getValue()) < 0) {
                                Map.Entry<T, Integer> temp = entry1;
                                entry1 = entry2;
                                entry2 = temp;
                            }
                        }

                        for (int i = 0; i < entry1.getValue(); i++) {
                            Document document = new Document();
                            document.add(luceneFieldFactory.apply(entry1.getKey(), true));
                            if (entry2 != null && i < entry2.getValue()) {
                                document.add(luceneFieldFactory.apply(entry2.getKey(), true));
                            }
                            indexWriter.addDocument(document);
                        }
                    }
                }
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    boolean order = randomBoolean();
                    List<Map.Entry<T, Integer>> expectedBuckets = new ArrayList<>();
                    expectedBuckets.addAll(counts.entrySet());
                    BucketOrder bucketOrder;
                    Comparator<Map.Entry<T, Integer>> comparator;
                    if (randomBoolean()) {
                        bucketOrder = BucketOrder.key(order);
                        comparator = Comparator.comparing(Map.Entry::getKey, keyComparator);
                    } else {
                        // if order by count then we need to use compound so that we can also sort by key as tie breaker:
                        bucketOrder = BucketOrder.compound(BucketOrder.count(order), BucketOrder.key(order));
                        comparator = Comparator.comparing(Map.Entry::getValue);
                        comparator = comparator.thenComparing(Comparator.comparing(Map.Entry::getKey, keyComparator));
                    }
                    if (order == false) {
                        comparator = comparator.reversed();
                    }
                    expectedBuckets.sort(comparator);
                    int size = randomIntBetween(1, counts.size());

                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    logger.info("bucket_order={} size={} execution_hint={}", bucketOrder, size, executionHint);
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(valueType)
                        .executionHint(executionHint)
                        .size(size)
                        .shardSize(size)
                        .field("field")
                        .order(bucketOrder);

                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(size, result.getBuckets().size());
                    for (int i = 0; i < size; i++) {
                        Map.Entry<T, Integer> expected = expectedBuckets.get(i);
                        Terms.Bucket actual = result.getBuckets().get(i);
                        if (valueType == ValueType.IP) {
                            assertEquals(String.valueOf(expected.getKey()).substring(1), actual.getKey());
                        } else {
                            assertEquals(expected.getKey(), actual.getKey());
                        }
                        assertEquals(expected.getValue().longValue(), actual.getDocCount());
                    }

                    if (multiValued == false) {
                        MappedFieldType filterFieldType = new KeywordFieldMapper.KeywordFieldType("include");
                        aggregationBuilder = new FilterAggregationBuilder("_name1", QueryBuilders.termQuery("include", "yes"));
                        aggregationBuilder.subAggregation(
                            new TermsAggregationBuilder("_name2").userValueTypeHint(valueType)
                                .executionHint(executionHint)
                                .size(numTerms)
                                .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
                                .field("field")
                        );
                        aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType, filterFieldType);
                        aggregator.preCollection();
                        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                        aggregator.postCollection();
                        result = ((Filter) reduce(aggregator)).getAggregations().get("_name2");
                        int expectedFilteredCounts = 0;
                        for (Integer count : filteredCounts.values()) {
                            if (count > 0) {
                                expectedFilteredCounts++;
                            }
                        }
                        assertEquals(expectedFilteredCounts, result.getBuckets().size());
                        for (Terms.Bucket actual : result.getBuckets()) {
                            Integer expectedCount;
                            if (valueType == ValueType.IP) {
                                expectedCount = filteredCounts.get(InetAddresses.forString((String) actual.getKey()));
                            } else {
                                expectedCount = filteredCounts.get(actual.getKey());
                            }
                            assertEquals(expectedCount.longValue(), actual.getDocCount());
                        }
                    }
                }
            }
        }
    }

    private <T> void termsAggregatorWithNestedMaxAgg(
        ValueType valueType,
        MappedFieldType fieldType,
        Function<Integer, T> valueFactory,
        Function<T, IndexableField> luceneFieldFactory
    ) throws Exception {
        final Map<T, Long> counts = new HashMap<>();
        int numTerms = scaledRandomIntBetween(8, 128);
        for (int i = 0; i < numTerms; i++) {
            counts.put(valueFactory.apply(i), randomLong());
        }

        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (Map.Entry<T, Long> entry : counts.entrySet()) {
                    Document document = new Document();
                    document.add(luceneFieldFactory.apply(entry.getKey()));
                    document.add(new NumericDocValuesField("value", entry.getValue()));
                    indexWriter.addDocument(document);
                }
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    boolean order = randomBoolean();
                    List<Map.Entry<T, Long>> expectedBuckets = new ArrayList<>();
                    expectedBuckets.addAll(counts.entrySet());
                    BucketOrder bucketOrder = BucketOrder.aggregation("_max", order);
                    Comparator<Map.Entry<T, Long>> comparator = Comparator.comparing(Map.Entry::getValue, Long::compareTo);
                    if (order == false) {
                        comparator = comparator.reversed();
                    }
                    expectedBuckets.sort(comparator);
                    int size = randomIntBetween(1, counts.size());

                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
                    logger.info(
                        "bucket_order={} size={} execution_hint={}, collect_mode={}",
                        bucketOrder,
                        size,
                        executionHint,
                        collectionMode
                    );
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    AggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(valueType)
                        .executionHint(executionHint)
                        .collectMode(collectionMode)
                        .size(size)
                        .shardSize(size)
                        .field("field")
                        .order(bucketOrder)
                        .subAggregation(AggregationBuilders.max("_max").field("value"));

                    MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("value", NumberFieldMapper.NumberType.LONG);
                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(size, result.getBuckets().size());
                    for (int i = 0; i < size; i++) {
                        Map.Entry<T, Long> expected = expectedBuckets.get(i);
                        Terms.Bucket actual = result.getBuckets().get(i);
                        assertEquals(expected.getKey(), actual.getKey());
                    }
                }
            }
        }
    }

    public void testEmpty() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("string");
                MappedFieldType fieldType2 = new NumberFieldMapper.NumberFieldType("long", NumberFieldMapper.NumberType.LONG);
                MappedFieldType fieldType3 = new NumberFieldMapper.NumberFieldType("double", NumberFieldMapper.NumberType.DOUBLE);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.STRING)
                        .field("string");
                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.LONG).field("long");
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());

                    aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.DOUBLE).field("double");
                    aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType3);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    result = reduce(aggregator);
                    assertEquals("_name", result.getName());
                    assertEquals(0, result.getBuckets().size());
                }
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    ValueType[] valueTypes = new ValueType[] { ValueType.STRING, ValueType.LONG, ValueType.DOUBLE };
                    String[] fieldNames = new String[] { "string", "long", "double" };
                    for (int i = 0; i < fieldNames.length; i++) {
                        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(valueTypes[i])
                            .field(fieldNames[i]);
                        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, (MappedFieldType) null);
                        aggregator.preCollection();
                        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                        aggregator.postCollection();
                        Terms result = reduce(aggregator);
                        assertEquals("_name", result.getName());
                        assertEquals(0, result.getBuckets().size());
                        assertFalse(AggregationInspectionHelper.hasValue((InternalTerms) result));
                    }
                }
            }
        }
    }

    public void testUnmappedWithMissing() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {

                Document document = new Document();
                document.add(new NumericDocValuesField("unrelated_value", 100));
                indexWriter.addDocument(document);

                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {

                    MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("unrelated_value");

                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    ValueType[] valueTypes = new ValueType[] { ValueType.STRING, ValueType.LONG, ValueType.DOUBLE };
                    String[] fieldNames = new String[] { "string", "long", "double" };
                    Object[] missingValues = new Object[] { "abc", 19L, 19.2 };

                    for (int i = 0; i < fieldNames.length; i++) {
                        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(valueTypes[i])
                            .field(fieldNames[i])
                            .missing(missingValues[i]);
                        Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1);
                        aggregator.preCollection();
                        indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                        aggregator.postCollection();
                        Terms result = reduce(aggregator);
                        assertEquals("_name", result.getName());
                        assertEquals(1, result.getBuckets().size());
                        assertEquals(missingValues[i], result.getBuckets().get(0).getKey());
                        assertEquals(1, result.getBuckets().get(0).getDocCount());
                    }
                }
            }
        }
    }

    public void testRangeField() throws Exception {
        try (Directory directory = newDirectory()) {
            double start = randomDouble();
            double end = randomDoubleBetween(Math.nextUp(start), Double.MAX_VALUE, false);
            RangeType rangeType = RangeType.DOUBLE;
            final RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType, start, end, true, true);
            final String fieldName = "field";
            final BinaryDocValuesField field = new BinaryDocValuesField(fieldName, rangeType.encodeRanges(Collections.singleton(range)));
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(field);
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    MappedFieldType fieldType = new RangeFieldMapper.RangeFieldType(fieldName, rangeType);
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field(fieldName);
                    expectThrows(IllegalArgumentException.class, () -> { createAggregator(aggregationBuilder, indexSearcher, fieldType); });
                }
            }
        }
    }

    public void testGeoPointField() throws Exception {
        try (Directory directory = newDirectory()) {
            GeoPoint point = RandomGeoGenerator.randomPoint(random());
            final String field = "field";
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new LatLonDocValuesField(field, point.getLat(), point.getLon()));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType("field");
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field(field);
                    expectThrows(IllegalArgumentException.class, () -> { createAggregator(aggregationBuilder, indexSearcher, fieldType); });
                }
            }
        }
    }

    public void testIpField() throws Exception {
        try (Directory directory = newDirectory()) {
            final String field = "field";
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(
                    new SortedSetDocValuesField("field", new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.100.42"))))
                );
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    MappedFieldType fieldType = new IpFieldMapper.IpFieldType("field");
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").field(field);
                    // Note - other places we throw IllegalArgumentException
                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals("_name", result.getName());
                    assertEquals(1, result.getBuckets().size());
                    assertEquals("192.168.100.42", result.getBuckets().get(0).getKey());
                    assertEquals(1, result.getBuckets().get(0).getDocCount());
                }
            }
        }
    }

    public void testNestedTermsAgg() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("a")));
                document.add(new SortedDocValuesField("field2", new BytesRef("b")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("c")));
                document.add(new SortedDocValuesField("field2", new BytesRef("d")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("field1", new BytesRef("e")));
                document.add(new SortedDocValuesField("field2", new BytesRef("f")));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
                    TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name1").userValueTypeHint(ValueType.STRING)
                        .executionHint(executionHint)
                        .collectMode(collectionMode)
                        .field("field1")
                        .order(BucketOrder.key(true))
                        .subAggregation(
                            new TermsAggregationBuilder("_name2").userValueTypeHint(ValueType.STRING)
                                .executionHint(executionHint)
                                .collectMode(collectionMode)
                                .field("field2")
                                .order(BucketOrder.key(true))
                        );
                    MappedFieldType fieldType1 = new KeywordFieldMapper.KeywordFieldType("field1");
                    MappedFieldType fieldType2 = new KeywordFieldMapper.KeywordFieldType("field2");

                    Aggregator aggregator = createAggregator(aggregationBuilder, indexSearcher, fieldType1, fieldType2);
                    aggregator.preCollection();
                    indexSearcher.search(new MatchAllDocsQuery(), aggregator);
                    aggregator.postCollection();
                    Terms result = reduce(aggregator);
                    assertEquals(3, result.getBuckets().size());
                    assertEquals("a", result.getBuckets().get(0).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(0).getDocCount());
                    Terms.Bucket nestedBucket = ((Terms) result.getBuckets().get(0).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("b", nestedBucket.getKeyAsString());
                    assertEquals("c", result.getBuckets().get(1).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(1).getDocCount());
                    nestedBucket = ((Terms) result.getBuckets().get(1).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("d", nestedBucket.getKeyAsString());
                    assertEquals("e", result.getBuckets().get(2).getKeyAsString());
                    assertEquals(1L, result.getBuckets().get(2).getDocCount());
                    nestedBucket = ((Terms) result.getBuckets().get(2).getAggregations().get("_name2")).getBuckets().get(0);
                    assertEquals("f", nestedBucket.getKeyAsString());
                }
            }
        }
    }

    public void testMixLongAndDouble() throws Exception {
        for (TermsAggregatorFactory.ExecutionMode executionMode : TermsAggregatorFactory.ExecutionMode.values()) {
            TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("_name").userValueTypeHint(ValueType.LONG)
                .executionHint(executionMode.toString())
                .field("number")
                .order(BucketOrder.key(true));
            List<InternalAggregation> aggs = new ArrayList<>();
            int numLongs = randomIntBetween(1, 3);
            for (int i = 0; i < numLongs; i++) {
                final Directory dir;
                try (IndexReader reader = createIndexWithLongs()) {
                    dir = ((DirectoryReader) reader).directory();
                    IndexSearcher searcher = new IndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
                    aggs.add(buildInternalAggregation(aggregationBuilder, fieldType, searcher));
                }
                dir.close();
            }
            int numDoubles = randomIntBetween(1, 3);
            for (int i = 0; i < numDoubles; i++) {
                final Directory dir;
                try (IndexReader reader = createIndexWithDoubles()) {
                    dir = ((DirectoryReader) reader).directory();
                    IndexSearcher searcher = new IndexSearcher(reader);
                    MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.DOUBLE);
                    aggs.add(buildInternalAggregation(aggregationBuilder, fieldType, searcher));
                }
                dir.close();
            }
            InternalAggregation.ReduceContext ctx = InternalAggregation.ReduceContext.forFinalReduction(
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
                null,
                b -> {},
                PipelineTree.EMPTY
            );
            for (InternalAggregation internalAgg : aggs) {
                InternalAggregation mergedAggs = internalAgg.reduce(aggs, ctx);
                assertTrue(mergedAggs instanceof DoubleTerms);
                long expected = numLongs + numDoubles;
                List<? extends Terms.Bucket> buckets = ((DoubleTerms) mergedAggs).getBuckets();
                assertEquals(4, buckets.size());
                assertEquals("1.0", buckets.get(0).getKeyAsString());
                assertEquals(expected, buckets.get(0).getDocCount());
                assertEquals("10.0", buckets.get(1).getKeyAsString());
                assertEquals(expected * 2, buckets.get(1).getDocCount());
                assertEquals("100.0", buckets.get(2).getKeyAsString());
                assertEquals(expected * 2, buckets.get(2).getDocCount());
                assertEquals("1000.0", buckets.get(3).getKeyAsString());
                assertEquals(expected, buckets.get(3).getDocCount());
            }
        }
    }

    public void testGlobalAggregationWithScore() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                Document document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("a")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("c")));
                indexWriter.addDocument(document);
                document = new Document();
                document.add(new SortedDocValuesField("keyword", new BytesRef("e")));
                indexWriter.addDocument(document);
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    String executionHint = randomFrom(TermsAggregatorFactory.ExecutionMode.values()).toString();
                    Aggregator.SubAggCollectionMode collectionMode = randomFrom(Aggregator.SubAggCollectionMode.values());
                    GlobalAggregationBuilder globalBuilder = new GlobalAggregationBuilder("global").subAggregation(
                        new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.STRING)
                            .executionHint(executionHint)
                            .collectMode(collectionMode)
                            .field("keyword")
                            .order(BucketOrder.key(true))
                            .subAggregation(
                                new TermsAggregationBuilder("sub_terms").userValueTypeHint(ValueType.STRING)
                                    .executionHint(executionHint)
                                    .collectMode(collectionMode)
                                    .field("keyword")
                                    .order(BucketOrder.key(true))
                                    .subAggregation(new TopHitsAggregationBuilder("top_hits").storedField("_none_"))
                            )
                    );

                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("keyword");

                    InternalGlobal result = searchAndReduce(indexSearcher, new MatchAllDocsQuery(), globalBuilder, fieldType);
                    InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                    assertThat(terms.getBuckets().size(), equalTo(3));
                    for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
                        InternalMultiBucketAggregation<?, ?> subTerms = bucket.getAggregations().get("sub_terms");
                        assertThat(subTerms.getBuckets().size(), equalTo(1));
                        MultiBucketsAggregation.Bucket subBucket = subTerms.getBuckets().get(0);
                        InternalTopHits topHits = subBucket.getAggregations().get("top_hits");
                        assertThat(topHits.getHits().getHits().length, equalTo(1));
                        for (SearchHit hit : topHits.getHits()) {
                            assertThat(hit.getScore(), greaterThan(0f));
                        }
                    }
                }
            }
        }
    }

    public void testWithNestedAggregations() throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < 10; i++) {
                    int[] nestedValues = new int[i];
                    for (int j = 0; j < i; j++) {
                        nestedValues[j] = j;
                    }
                    indexWriter.addDocuments(generateDocsWithNested(Integer.toString(i), i, nestedValues));
                }
                indexWriter.commit();
                for (Aggregator.SubAggCollectionMode mode : Aggregator.SubAggCollectionMode.values()) {
                    for (boolean withScore : new boolean[] { true, false }) {
                        NestedAggregationBuilder nested = new NestedAggregationBuilder("nested", "nested_object").subAggregation(
                            new TermsAggregationBuilder("terms").userValueTypeHint(ValueType.LONG)
                                .field("nested_value")
                                // force the breadth_first mode
                                .collectMode(mode)
                                .order(BucketOrder.key(true))
                                .subAggregation(
                                    new TopHitsAggregationBuilder("top_hits").sort(
                                        withScore ? new ScoreSortBuilder() : new FieldSortBuilder("_doc")
                                    ).storedField("_none_")
                                )
                        );
                        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(
                            "nested_value",
                            NumberFieldMapper.NumberType.LONG
                        );
                        try (IndexReader indexReader = wrapInMockESDirectoryReader(DirectoryReader.open(directory))) {
                            {
                                InternalNested result = searchAndReduce(
                                    newSearcher(indexReader, false, true),
                                    // match root document only
                                    new DocValuesFieldExistsQuery(PRIMARY_TERM_NAME),
                                    nested,
                                    fieldType
                                );
                                InternalMultiBucketAggregation<?, ?> terms = result.getAggregations().get("terms");
                                assertNestedTopHitsScore(terms, withScore);
                            }

                            {
                                FilterAggregationBuilder filter = new FilterAggregationBuilder("filter", new MatchAllQueryBuilder())
                                    .subAggregation(nested);
                                InternalFilter result = searchAndReduce(
                                    newSearcher(indexReader, false, true),
                                    // match root document only
                                    new DocValuesFieldExistsQuery(PRIMARY_TERM_NAME),
                                    filter,
                                    fieldType
                                );
                                InternalNested nestedResult = result.getAggregations().get("nested");
                                InternalMultiBucketAggregation<?, ?> terms = nestedResult.getAggregations().get("terms");
                                assertNestedTopHitsScore(terms, withScore);
                            }
                        }
                    }
                }
            }
        }
    }

    public void testNumberToStringValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.INTEGER);

        TermsAggregationBuilder aggregationBuilder = new TermsAggregationBuilder("name").userValueTypeHint(ValueType.STRING)
            .field("number")
            .script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, STRING_SCRIPT_NAME, Collections.emptyMap()));

        testCase(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            final int numDocs = 10;
            for (int i = 0; i < numDocs; i++) {
                iw.addDocument(singleton(new NumericDocValuesField("number", i + 1)));
            }
        }, (Consumer<InternalTerms>) terms -> { assertTrue(AggregationInspectionHelper.hasValue(terms)); }, fieldType);
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
                    TermsAggregationBuilder request = new TermsAggregationBuilder("i").field("i")
                        .executionHint(executionHint)
                        .subAggregation(
                            new TermsAggregationBuilder("j").field("j")
                                .executionHint(executionHint)
                                .subAggregation(new TermsAggregationBuilder("k").field("k").executionHint(executionHint))
                        );
                    StringTerms result = searchAndReduce(
                        searcher,
                        new MatchAllDocsQuery(),
                        request,
                        keywordField("i"),
                        keywordField("j"),
                        keywordField("k")
                    );
                    for (int i = 0; i < 10; i++) {
                        StringTerms.Bucket iBucket = result.getBucketByKey(Integer.toString(i));
                        assertThat(iBucket.getDocCount(), equalTo(100L));
                        StringTerms jAgg = iBucket.getAggregations().get("j");
                        for (int j = 0; j < 10; j++) {
                            StringTerms.Bucket jBucket = jAgg.getBucketByKey(Integer.toString(j));
                            assertThat(jBucket.getDocCount(), equalTo(10L));
                            StringTerms kAgg = jBucket.getAggregations().get("k");
                            for (int k = 0; k < 10; k++) {
                                StringTerms.Bucket kBucket = kAgg.getBucketByKey(Integer.toString(k));
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
                    TermsAggregationBuilder request = new TermsAggregationBuilder("i").field("i")
                        .subAggregation(
                            new TermsAggregationBuilder("j").field("j").subAggregation(new TermsAggregationBuilder("k").field("k"))
                        );
                    LongTerms result = searchAndReduce(
                        searcher,
                        new MatchAllDocsQuery(),
                        request,
                        longField("i"),
                        longField("j"),
                        longField("k")
                    );
                    for (int i = 0; i < 10; i++) {
                        LongTerms.Bucket iBucket = result.getBucketByKey(Integer.toString(i));
                        assertThat(iBucket.getDocCount(), equalTo(100L));
                        LongTerms jAgg = iBucket.getAggregations().get("j");
                        for (int j = 0; j < 10; j++) {
                            LongTerms.Bucket jBucket = jAgg.getBucketByKey(Integer.toString(j));
                            assertThat(jBucket.getDocCount(), equalTo(10L));
                            LongTerms kAgg = jBucket.getAggregations().get("k");
                            for (int k = 0; k < 10; k++) {
                                LongTerms.Bucket kBucket = kAgg.getBucketByKey(Integer.toString(k));
                                assertThat(kBucket.getDocCount(), equalTo(1L));
                            }
                        }
                    }
                }
            }
        }
    }

    private void assertNestedTopHitsScore(InternalMultiBucketAggregation<?, ?> terms, boolean withScore) {
        assertThat(terms.getBuckets().size(), equalTo(9));
        int ptr = 9;
        for (MultiBucketsAggregation.Bucket bucket : terms.getBuckets()) {
            InternalTopHits topHits = bucket.getAggregations().get("top_hits");
            assertThat(topHits.getHits().getTotalHits().value, equalTo((long) ptr));
            assertEquals(TotalHits.Relation.EQUAL_TO, topHits.getHits().getTotalHits().relation);
            if (withScore) {
                assertThat(topHits.getHits().getMaxScore(), equalTo(1f));
            } else {
                assertThat(topHits.getHits().getMaxScore(), equalTo(Float.NaN));
            }
            --ptr;
        }
    }

    public void testOrderByPipelineAggregation() throws Exception {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory)) {
                try (IndexReader indexReader = maybeWrapReaderEs(indexWriter.getReader())) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                    BucketScriptPipelineAggregationBuilder bucketScriptAgg = bucketScript("script", new Script("2.718"));
                    TermsAggregationBuilder termsAgg = terms("terms").field("field")
                        .userValueTypeHint(ValueType.STRING)
                        .order(BucketOrder.aggregation("script", true))
                        .subAggregation(bucketScriptAgg);

                    MappedFieldType fieldType = new KeywordFieldMapper.KeywordFieldType("field");

                    AggregationExecutionException e = expectThrows(
                        AggregationExecutionException.class,
                        () -> createAggregator(termsAgg, indexSearcher, fieldType)
                    );
                    assertEquals(
                        "Invalid aggregation order path [script]. The provided aggregation [script] "
                            + "either does not exist, or is a pipeline aggregation and cannot be used to sort the buckets.",
                        e.getMessage()
                    );
                }
            }
        }
    }

    private final SeqNoFieldMapper.SequenceIDFields sequenceIDFields = SeqNoFieldMapper.SequenceIDFields.emptySeqID();

    private List<Document> generateDocsWithNested(String id, int value, int[] nestedValues) {
        List<Document> documents = new ArrayList<>();

        for (int nestedValue : nestedValues) {
            Document document = new Document();
            document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.NESTED_FIELD_TYPE));
            document.add(new Field(NestedPathFieldMapper.NAME, "nested_object", NestedPathFieldMapper.Defaults.FIELD_TYPE));
            document.add(new SortedNumericDocValuesField("nested_value", nestedValue));
            documents.add(document);
        }

        Document document = new Document();
        document.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        document.add(new SortedNumericDocValuesField("value", value));
        document.add(sequenceIDFields.primaryTerm);
        documents.add(document);

        return documents;
    }

    private IndexReader createIndexWithLongs() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", 10));
        document.add(new SortedNumericDocValuesField("number", 100));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 1));
        document.add(new SortedNumericDocValuesField("number", 100));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", 10));
        document.add(new SortedNumericDocValuesField("number", 1000));
        indexWriter.addDocument(document);
        indexWriter.close();
        return DirectoryReader.open(directory);
    }

    private IndexReader createIndexWithDoubles() throws IOException {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        Document document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(10.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(100.0d)));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(1.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(100.0d)));
        indexWriter.addDocument(document);
        document = new Document();
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(10.0d)));
        document.add(new SortedNumericDocValuesField("number", NumericUtils.doubleToSortableLong(1000.0d)));
        indexWriter.addDocument(document);
        indexWriter.close();
        return DirectoryReader.open(directory);
    }

    private InternalAggregation buildInternalAggregation(TermsAggregationBuilder builder, MappedFieldType fieldType, IndexSearcher searcher)
        throws IOException {
        TermsAggregator aggregator = createAggregator(builder, searcher, fieldType);
        aggregator.preCollection();
        searcher.search(new MatchAllDocsQuery(), aggregator);
        aggregator.postCollection();
        return aggregator.buildTopLevel();
    }

    private <T extends InternalAggregation> T reduce(Aggregator agg) throws IOException {
        // now do the final reduce
        MultiBucketConsumerService.MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumerService.MultiBucketConsumer(
            Integer.MAX_VALUE,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            agg.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            PipelineTree.EMPTY
        );

        T topLevel = (T) agg.buildTopLevel();
        T result = (T) topLevel.reduce(Collections.singletonList(topLevel), context);
        doAssertReducedMultiBucketConsumer(result, reduceBucketConsumer);
        return result;
    }
}
