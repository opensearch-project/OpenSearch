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

package org.opensearch.search.aggregations;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.index.CompositeReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.tests.search.AssertingIndexSearcher;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.TriFunction;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.core.xcontent.ContextParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.cache.bitset.BitsetFilterCache.Listener;
import org.opensearch.index.cache.query.DisabledQueryCache;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.BinaryFieldMapper;
import org.opensearch.index.mapper.CompletionFieldMapper;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.FieldAliasMapper;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.index.mapper.GeoShapeFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.Mapper.BuilderContext;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.ObjectMapper.Nested;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesModule;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.MultiBucketConsumerService.MultiBucketConsumer;
import org.opensearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.fetch.FetchPhase;
import org.opensearch.search.fetch.subphase.FetchDocValuesPhase;
import org.opensearch.search.fetch.subphase.FetchSourcePhase;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.InternalAggregationTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.opensearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base class for testing {@link Aggregator} implementations.
 * Provides helpers for constructing and searching an {@link Aggregator} implementation based on a provided
 * {@link AggregationBuilder} instance.
 */
public abstract class AggregatorTestCase extends OpenSearchTestCase {
    private static final String NESTEDFIELD_PREFIX = "nested_";
    private List<Releasable> releasables = new ArrayList<>();
    private static final String TYPE_NAME = "type";
    protected ValuesSourceRegistry valuesSourceRegistry;

    // A list of field types that should not be tested, or are not currently supported
    private static List<String> TYPE_TEST_DENYLIST;

    static {
        List<String> denylist = new ArrayList<>();
        denylist.add(ObjectMapper.CONTENT_TYPE); // Cannot aggregate objects
        denylist.add(GeoShapeFieldMapper.CONTENT_TYPE); // Cannot aggregate geoshapes (yet)
        denylist.add(ObjectMapper.NESTED_CONTENT_TYPE); // TODO support for nested
        denylist.add(CompletionFieldMapper.CONTENT_TYPE); // TODO support completion
        denylist.add(FieldAliasMapper.CONTENT_TYPE); // TODO support alias
        TYPE_TEST_DENYLIST = denylist;
    }

    /**
     * Allows subclasses to provide alternate names for the provided field type, which
     * can be useful when testing aggregations on field aliases.
     */
    protected Map<String, MappedFieldType> getFieldAliases(MappedFieldType... fieldTypes) {
        return Collections.emptyMap();
    }

    private static void registerFieldTypes(
        SearchContext searchContext,
        MapperService mapperService,
        Map<String, MappedFieldType> fieldNameToType
    ) {
        for (Map.Entry<String, MappedFieldType> entry : fieldNameToType.entrySet()) {
            String fieldName = entry.getKey();
            MappedFieldType fieldType = entry.getValue();

            when(mapperService.fieldType(fieldName)).thenReturn(fieldType);
            when(searchContext.fieldType(fieldName)).thenReturn(fieldType);
        }
    }

    // Make this @Before instead of @BeforeClass so it can call the non-static getSearchPlugins method
    @Before
    public void initValuesSourceRegistry() {
        List<SearchPlugin> plugins = new ArrayList<>(getSearchPlugins());
        plugins.add(new AggCardinalityPlugin());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, plugins);
        valuesSourceRegistry = searchModule.getValuesSourceRegistry();
    }

    /**
     * Test cases should override this if they have plugins that need to be loaded, e.g. the plugins their aggregators are in.
     */
    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.emptyList();
    }

    protected <A extends Aggregator> A createAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(
            aggregationBuilder,
            indexSearcher,
            createIndexSettings(),
            new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)),
            fieldTypes
        );
    }

    protected <A extends Aggregator> A createAggregator(
        Query query,
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(
            query,
            aggregationBuilder,
            indexSearcher,
            indexSettings,
            new MultiBucketConsumer(DEFAULT_MAX_BUCKETS, new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)),
            fieldTypes
        );
    }

    protected <A extends Aggregator> A createAggregator(
        Query query,
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        MultiBucketConsumer bucketConsumer,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(query, aggregationBuilder, indexSearcher, createIndexSettings(), bucketConsumer, fieldTypes);
    }

    protected <A extends Aggregator> A createAggregator(
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        MultiBucketConsumer bucketConsumer,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createAggregator(null, aggregationBuilder, indexSearcher, indexSettings, bucketConsumer, fieldTypes);
    }

    protected <A extends Aggregator> A createAggregator(
        Query query,
        AggregationBuilder aggregationBuilder,
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        MultiBucketConsumer bucketConsumer,
        MappedFieldType... fieldTypes
    ) throws IOException {
        SearchContext searchContext = createSearchContext(indexSearcher, indexSettings, query, bucketConsumer, fieldTypes);
        return createAggregator(aggregationBuilder, searchContext);
    }

    protected <A extends Aggregator> A createAggregator(AggregationBuilder aggregationBuilder, SearchContext searchContext)
        throws IOException {
        @SuppressWarnings("unchecked")
        A aggregator = (A) aggregationBuilder.rewrite(searchContext.getQueryShardContext())
            .build(searchContext.getQueryShardContext(), null)
            .create(searchContext, null, CardinalityUpperBound.ONE);
        return aggregator;
    }

    /**
     * Create a {@linkplain SearchContext} for testing an {@link Aggregator}.
     */
    protected SearchContext createSearchContext(
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        Query query,
        MultiBucketConsumer bucketConsumer,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return createSearchContext(indexSearcher, indexSettings, query, bucketConsumer, new NoneCircuitBreakerService(), fieldTypes);
    }

    protected SearchContext createSearchContext(
        IndexSearcher indexSearcher,
        IndexSettings indexSettings,
        Query query,
        MultiBucketConsumer bucketConsumer,
        CircuitBreakerService circuitBreakerService,
        MappedFieldType... fieldTypes
    ) throws IOException {
        QueryCache queryCache = new DisabledQueryCache(indexSettings);
        QueryCachingPolicy queryCachingPolicy = new QueryCachingPolicy() {
            @Override
            public void onUse(Query query) {}

            @Override
            public boolean shouldCache(Query query) {
                // never cache a query
                return false;
            }
        };
        SearchContext searchContext = mock(SearchContext.class);
        ContextIndexSearcher contextIndexSearcher = new ContextIndexSearcher(
            indexSearcher.getIndexReader(),
            indexSearcher.getSimilarity(),
            queryCache,
            queryCachingPolicy,
            false,
            null,
            searchContext
        );

        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searcher()).thenReturn(contextIndexSearcher);
        when(searchContext.fetchPhase()).thenReturn(new FetchPhase(Arrays.asList(new FetchSourcePhase(), new FetchDocValuesPhase())));
        when(searchContext.bitsetFilterCache()).thenReturn(new BitsetFilterCache(indexSettings, mock(Listener.class)));
        IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(new ShardId("test", "test", 0));
        when(searchContext.indexShard()).thenReturn(indexShard);
        when(searchContext.aggregations()).thenReturn(new SearchContextAggregations(AggregatorFactories.EMPTY, bucketConsumer));
        when(searchContext.query()).thenReturn(query);
        when(searchContext.bucketCollectorProcessor()).thenReturn(new BucketCollectorProcessor());
        /*
         * Always use the circuit breaking big arrays instance so that the CircuitBreakerService
         * we're passed gets a chance to break.
         */
        BigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), circuitBreakerService).withCircuitBreaking();
        when(searchContext.bigArrays()).thenReturn(bigArrays);

        // TODO: now just needed for top_hits, this will need to be revised for other agg unit tests:
        MapperService mapperService = mapperServiceMock();
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        when(mapperService.hasNested()).thenReturn(false);
        when(searchContext.mapperService()).thenReturn(mapperService);
        IndexFieldDataService ifds = new IndexFieldDataService(
            indexSettings,
            new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
            }),
            circuitBreakerService,
            mapperService
        );
        QueryShardContext queryShardContext = queryShardContextMock(
            contextIndexSearcher,
            mapperService,
            indexSettings,
            circuitBreakerService,
            bigArrays
        );
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);
        when(queryShardContext.getObjectMapper(anyString())).thenAnswer(invocation -> {
            String fieldName = (String) invocation.getArguments()[0];
            if (fieldName.startsWith(NESTEDFIELD_PREFIX)) {
                BuilderContext context = new BuilderContext(indexSettings.getSettings(), new ContentPath());
                return new ObjectMapper.Builder<>(fieldName).nested(Nested.newNested()).build(context);
            }
            return null;
        });

        Map<String, MappedFieldType> fieldNameToType = new HashMap<>();
        fieldNameToType.putAll(
            Arrays.stream(fieldTypes).filter(Objects::nonNull).collect(Collectors.toMap(MappedFieldType::name, Function.identity()))
        );
        fieldNameToType.putAll(getFieldAliases(fieldTypes));

        registerFieldTypes(searchContext, mapperService, fieldNameToType);
        doAnswer(invocation -> {
            /* Store the release-ables so we can release them at the end of the test case. This is important because aggregations don't
             * close their sub-aggregations. This is fairly similar to what the production code does. */
            releasables.add((Releasable) invocation.getArguments()[0]);
            return null;
        }).when(searchContext).addReleasable(any());
        return searchContext;
    }

    protected IndexSettings createIndexSettings() {
        return new IndexSettings(
            IndexMetadata.builder("_index")
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .creationDate(System.currentTimeMillis())
                .build(),
            Settings.EMPTY
        );
    }

    /**
     * sub-tests that need a more complex mock can overwrite this
     */
    protected MapperService mapperServiceMock() {
        return mock(MapperService.class);
    }

    /**
     * sub-tests that need a more complex mock can overwrite this
     */
    protected QueryShardContext queryShardContextMock(
        IndexSearcher searcher,
        MapperService mapperService,
        IndexSettings indexSettings,
        CircuitBreakerService circuitBreakerService,
        BigArrays bigArrays
    ) {

        return new QueryShardContext(
            0,
            indexSettings,
            bigArrays,
            null,
            getIndexFieldDataLookup(mapperService, circuitBreakerService),
            mapperService,
            null,
            getMockScriptService(),
            xContentRegistry(),
            writableRegistry(),
            null,
            searcher,
            System::currentTimeMillis,
            null,
            null,
            () -> true,
            valuesSourceRegistry
        );
    }

    /**
     * Sub-tests that need a more complex index field data provider can override this
     */
    protected TriFunction<MappedFieldType, String, Supplier<SearchLookup>, IndexFieldData<?>> getIndexFieldDataLookup(
        MapperService mapperService,
        CircuitBreakerService circuitBreakerService
    ) {
        return (fieldType, s, searchLookup) -> fieldType.fielddataBuilder(
            mapperService.getIndexSettings().getIndex().getName(),
            searchLookup
        ).build(new IndexFieldDataCache.None(), circuitBreakerService);
    }

    /**
     * Sub-tests that need scripting can override this method to provide a script service and pre-baked scripts
     */
    protected ScriptService getMockScriptService() {
        return null;
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(createIndexSettings(), searcher, query, builder, DEFAULT_MAX_BUCKETS, fieldTypes);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(indexSettings, searcher, query, builder, DEFAULT_MAX_BUCKETS, fieldTypes);
    }

    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        MappedFieldType... fieldTypes
    ) throws IOException {
        return searchAndReduce(createIndexSettings(), searcher, query, builder, maxBucket, fieldTypes);
    }

    /**
     * Collects all documents that match the provided query {@link Query} and
     * returns the reduced {@link InternalAggregation}.
     * <p>
     * Half the time it aggregates each leaf individually and reduces all
     * results together. The other half the time it aggregates across the entire
     * index at once and runs a final reduction on the single resulting agg.
     */
    protected <A extends InternalAggregation, C extends Aggregator> A searchAndReduce(
        IndexSettings indexSettings,
        IndexSearcher searcher,
        Query query,
        AggregationBuilder builder,
        int maxBucket,
        MappedFieldType... fieldTypes
    ) throws IOException {
        final IndexReaderContext ctx = searcher.getTopReaderContext();
        final PipelineTree pipelines = builder.buildPipelineTree();
        List<InternalAggregation> aggs = new ArrayList<>();
        Query rewritten = searcher.rewrite(query);
        MultiBucketConsumer bucketConsumer = new MultiBucketConsumer(
            maxBucket,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        C root = createAggregator(query, builder, searcher, bucketConsumer, fieldTypes);

        if (randomBoolean() && searcher.getIndexReader().leaves().size() > 0) {
            assertThat(ctx, instanceOf(CompositeReaderContext.class));
            final CompositeReaderContext compCTX = (CompositeReaderContext) ctx;
            final int size = compCTX.leaves().size();
            final ShardSearcher[] subSearchers = new ShardSearcher[size];
            for (int searcherIDX = 0; searcherIDX < subSearchers.length; searcherIDX++) {
                final LeafReaderContext leave = compCTX.leaves().get(searcherIDX);
                subSearchers[searcherIDX] = new ShardSearcher(leave, compCTX);
            }
            for (ShardSearcher subSearcher : subSearchers) {
                MultiBucketConsumer shardBucketConsumer = new MultiBucketConsumer(
                    maxBucket,
                    new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
                );
                C a = createAggregator(query, builder, subSearcher, indexSettings, shardBucketConsumer, fieldTypes);
                a.preCollection();
                Weight weight = subSearcher.createWeight(rewritten, ScoreMode.COMPLETE, 1f);
                subSearcher.search(weight, a);
                a.postCollection();
                aggs.add(a.buildTopLevel());
            }
        } else {
            root.preCollection();
            searcher.search(rewritten, root);
            root.postCollection();
            aggs.add(root.buildTopLevel());
        }

        if (randomBoolean() && aggs.size() > 1) {
            // sometimes do an incremental reduce
            int toReduceSize = aggs.size();
            Collections.shuffle(aggs, random());
            int r = randomIntBetween(1, toReduceSize);
            List<InternalAggregation> toReduce = aggs.subList(0, r);
            InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forPartialReduction(
                root.context().bigArrays(),
                getMockScriptService(),
                () -> PipelineAggregator.PipelineTree.EMPTY
            );
            A reduced = (A) aggs.get(0).reduce(toReduce, context);
            aggs = new ArrayList<>(aggs.subList(r, toReduceSize));
            aggs.add(reduced);
        }

        // now do the final reduce
        MultiBucketConsumer reduceBucketConsumer = new MultiBucketConsumer(
            maxBucket,
            new NoneCircuitBreakerService().getBreaker(CircuitBreaker.REQUEST)
        );
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            root.context().bigArrays(),
            getMockScriptService(),
            reduceBucketConsumer,
            pipelines
        );

        @SuppressWarnings("unchecked")
        A internalAgg = (A) aggs.get(0).reduce(aggs, context);

        // materialize any parent pipelines
        internalAgg = (A) internalAgg.reducePipelines(internalAgg, context, pipelines);

        // materialize any sibling pipelines at top level
        for (PipelineAggregator pipelineAggregator : pipelines.aggregators()) {
            internalAgg = (A) pipelineAggregator.reduce(internalAgg, context);
        }
        doAssertReducedMultiBucketConsumer(internalAgg, reduceBucketConsumer);
        return internalAgg;
    }

    protected void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        InternalAggregationTestCase.assertMultiBucketConsumer(agg, bucketConsumer);
    }

    protected <T extends AggregationBuilder, V extends InternalAggregation> void testCase(
        T aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<V> verify,
        MappedFieldType... fieldTypes
    ) throws IOException {
        try (Directory directory = newDirectory()) {
            RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
            buildIndex.accept(indexWriter);
            indexWriter.close();

            try (DirectoryReader unwrapped = DirectoryReader.open(directory); IndexReader indexReader = wrapDirectoryReader(unwrapped)) {
                IndexSearcher indexSearcher = newIndexSearcher(indexReader);

                V agg = searchAndReduce(indexSearcher, query, aggregationBuilder, fieldTypes);
                verify.accept(agg);
            }
        }
    }

    /**
     * Override to wrap the {@linkplain DirectoryReader} for aggs like
     * {@link NestedAggregationBuilder}.
     */
    protected IndexReader wrapDirectoryReader(DirectoryReader reader) throws IOException {
        return reader;
    }

    private static class ShardSearcher extends IndexSearcher {
        private final List<LeafReaderContext> ctx;

        ShardSearcher(LeafReaderContext ctx, IndexReaderContext parent) {
            super(parent);
            this.ctx = Collections.singletonList(ctx);
        }

        public void search(Weight weight, Collector collector) throws IOException {
            search(ctx, weight, collector);
        }

        @Override
        public String toString() {
            return "ShardSearcher(" + ctx.get(0) + ")";
        }
    }

    protected static DirectoryReader wrapInMockESDirectoryReader(DirectoryReader directoryReader) throws IOException {
        return OpenSearchDirectoryReader.wrap(directoryReader, new ShardId(new Index("_index", "_na_"), 0));
    }

    /**
     * Added to randomly run with more assertions on the index searcher level,
     * like {@link org.apache.lucene.tests.util.LuceneTestCase#newSearcher(IndexReader)}, which can't be used because it also
     * wraps in the IndexSearcher's IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexSearcher newIndexSearcher(IndexReader indexReader) {
        if (randomBoolean()) {
            // this executes basic query checks and asserts that weights are normalized only once etc.
            return new AssertingIndexSearcher(random(), indexReader);
        } else {
            return new IndexSearcher(indexReader);
        }
    }

    /**
     * Added to randomly run with more assertions on the index reader level,
     * like {@link org.apache.lucene.tests.util.LuceneTestCase#wrapReader(IndexReader)}, which can't be used because it also
     * wraps in the IndexReader with other implementations that we can't handle. (e.g. ParallelCompositeReader)
     */
    protected static IndexReader maybeWrapReaderEs(DirectoryReader reader) throws IOException {
        if (randomBoolean()) {
            return new AssertingDirectoryReader(reader);
        } else {
            return reader;
        }
    }

    /**
     * Implementors should return a list of {@link ValuesSourceType} that the aggregator supports.
     * This is used to test the matrix of supported/unsupported field types against the aggregator
     * and verify it works (or doesn't) as expected.
     *
     * If this method is implemented, {@link AggregatorTestCase#createAggBuilderForTypeTest(MappedFieldType, String)}
     * should be implemented as well.
     *
     * @return list of supported ValuesSourceTypes
     */
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // If aggs don't override this method, an empty list allows the test to be skipped.
        // Once all aggs implement this method we should make it abstract and not allow skipping.
        return Collections.emptyList();
    }

    /**
     * This method is invoked each time a field type is tested in {@link AggregatorTestCase#testSupportedFieldTypes()}.
     * The field type and name are provided, and the implementor is expected to return an AggBuilder accordingly.
     * The AggBuilder should be returned even if the aggregation does not support the field type, because
     * the test will check if an exception is thrown in that case.
     *
     * The list of supported types are provided by {@link AggregatorTestCase#getSupportedValuesSourceTypes()},
     * which must also be implemented.
     *
     * @param fieldType the type of the field that will be tested
     * @param fieldName the name of the field that will be test
     * @return an aggregation builder to test against the field
     */
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        throw new UnsupportedOperationException(
            "If getSupportedValuesSourceTypes() is implemented, " + "createAggBuilderForTypeTest() must be implemented as well."
        );
    }

    /**
     * A method that allows implementors to specifically denylist particular field types (based on their content_name).
     * This is needed in some areas where the ValuesSourceType is not granular enough, for example integer values
     * vs floating points, or `keyword` bytes vs `binary` bytes (which are not searchable)
     *
     * This is a denylist instead of an allowlist because there are vastly more field types than ValuesSourceTypes,
     * and it's expected that these unsupported cases are exceptional rather than common
     */
    protected List<String> unsupportedMappedFieldTypes() {
        return Collections.emptyList();
    }

    /**
     * This test will validate that an aggregator succeeds or fails to run against all the field types
     * that are registered in {@link IndicesModule} (e.g. all the core field types).  An aggregator
     * is provided by the implementor class, and it is executed against each field type in turn.  If
     * an exception is thrown when the field is supported, that will fail the test.  Similarly, if
     * an exception _is not_ thrown when a field is unsupported, that will also fail the test.
     *
     * Exception types/messages are not currently checked, just presence/absence of an exception.
     */
    public void testSupportedFieldTypes() throws IOException {
        MapperRegistry mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
        Settings settings = Settings.builder().put("index.version.created", Version.CURRENT.id).build();
        String fieldName = "typeTestFieldName";
        List<ValuesSourceType> supportedVSTypes = getSupportedValuesSourceTypes();
        List<String> unsupportedMappedFieldTypes = unsupportedMappedFieldTypes();

        if (supportedVSTypes.isEmpty()) {
            // If the test says it doesn't support any VStypes, it has not been converted yet so skip
            return;
        }

        for (Map.Entry<String, Mapper.TypeParser> mappedType : mapperRegistry.getMapperParsers().entrySet()) {

            // Some field types should not be tested, or require more work and are not ready yet
            if (TYPE_TEST_DENYLIST.contains(mappedType.getKey())) {
                continue;
            }

            Map<String, Object> source = new HashMap<>();
            source.put("type", mappedType.getKey());

            // Text is the only field that doesn't support DVs, instead FD
            if (mappedType.getKey().equals(TextFieldMapper.CONTENT_TYPE) == false) {
                source.put("doc_values", "true");
            }

            Mapper.Builder builder = mappedType.getValue().parse(fieldName, source, new MockParserContext());
            FieldMapper mapper = (FieldMapper) builder.build(new BuilderContext(settings, new ContentPath()));

            MappedFieldType fieldType = mapper.fieldType();

            // Non-aggregatable fields are not testable (they will throw an error on all aggs anyway), so skip
            if (fieldType.isAggregatable() == false) {
                continue;
            }

            try (Directory directory = newDirectory()) {
                RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
                writeTestDoc(fieldType, fieldName, indexWriter);
                indexWriter.close();

                try (IndexReader indexReader = DirectoryReader.open(directory)) {
                    IndexSearcher indexSearcher = newIndexSearcher(indexReader);
                    AggregationBuilder aggregationBuilder = createAggBuilderForTypeTest(fieldType, fieldName);

                    ValuesSourceType vst = fieldToVST(fieldType);
                    // TODO in the future we can make this more explicit with expectThrows(), when the exceptions are standardized
                    AssertionError failure = null;
                    try {
                        searchAndReduce(indexSearcher, new MatchAllDocsQuery(), aggregationBuilder, fieldType);
                        if (supportedVSTypes.contains(vst) == false || unsupportedMappedFieldTypes.contains(fieldType.typeName())) {
                            failure = new AssertionError(
                                "Aggregator ["
                                    + aggregationBuilder.getType()
                                    + "] should not support field type ["
                                    + fieldType.typeName()
                                    + "] but executing against the field did not throw an exception"
                            );
                        }
                    } catch (Exception | AssertionError e) {
                        if (supportedVSTypes.contains(vst) && unsupportedMappedFieldTypes.contains(fieldType.typeName()) == false) {
                            failure = new AssertionError(
                                "Aggregator ["
                                    + aggregationBuilder.getType()
                                    + "] supports field type ["
                                    + fieldType.typeName()
                                    + "] but executing against the field threw an exception: ["
                                    + e.getMessage()
                                    + "]",
                                e
                            );
                        }
                    }
                    if (failure != null) {
                        throw failure;
                    }
                }
            }
        }
    }

    private ValuesSourceType fieldToVST(MappedFieldType fieldType) {
        return fieldType.fielddataBuilder("", () -> { throw new UnsupportedOperationException(); }).build(null, null).getValuesSourceType();
    }

    /**
     * Helper method to write a single document with a single value specific to the requested fieldType.
     *
     * Throws an exception if it encounters an unknown field type, to prevent new ones from sneaking in without
     * being tested.
     */
    private void writeTestDoc(MappedFieldType fieldType, String fieldName, RandomIndexWriter iw) throws IOException {

        String typeName = fieldType.typeName();
        ValuesSourceType vst = fieldToVST(fieldType);
        Document doc = new Document();
        String json;

        if (vst.equals(CoreValuesSourceType.NUMERIC)) {
            long v;
            if (typeName.equals(NumberFieldMapper.NumberType.DOUBLE.typeName())) {
                double d = Math.abs(randomDouble());
                v = NumericUtils.doubleToSortableLong(d);
                json = "{ \"" + fieldName + "\" : \"" + d + "\" }";
            } else if (typeName.equals(NumberFieldMapper.NumberType.FLOAT.typeName())) {
                float f = Math.abs(randomFloat());
                v = NumericUtils.floatToSortableInt(f);
                json = "{ \"" + fieldName + "\" : \"" + f + "\" }";
            } else if (typeName.equals(NumberFieldMapper.NumberType.HALF_FLOAT.typeName())) {
                // Generate a random float that respects the limits of half float
                float f = Math.abs((randomFloat() * 2 - 1) * 65504);
                v = HalfFloatPoint.halfFloatToSortableShort(f);
                json = "{ \"" + fieldName + "\" : \"" + f + "\" }";
            } else {
                // smallest numeric is a byte so we select the smallest
                v = Math.abs(randomByte());
                json = "{ \"" + fieldName + "\" : \"" + v + "\" }";
            }
            doc.add(new SortedNumericDocValuesField(fieldName, v));

        } else if (vst.equals(CoreValuesSourceType.BYTES)) {
            if (typeName.equals(BinaryFieldMapper.CONTENT_TYPE)) {
                doc.add(new BinaryFieldMapper.CustomBinaryDocValuesField(fieldName, new BytesRef("a").bytes));
                json = "{ \"" + fieldName + "\" : \"a\" }";
            } else {
                doc.add(new SortedSetDocValuesField(fieldName, new BytesRef("a")));
                json = "{ \"" + fieldName + "\" : \"a\" }";
            }
        } else if (vst.equals(CoreValuesSourceType.DATE)) {
            // positive integer because date_nanos gets unhappy with large longs
            long v;
            v = Math.abs(randomInt());
            doc.add(new SortedNumericDocValuesField(fieldName, v));
            json = "{ \"" + fieldName + "\" : \"" + v + "\" }";
        } else if (vst.equals(CoreValuesSourceType.BOOLEAN)) {
            long v;
            v = randomBoolean() ? 0 : 1;
            doc.add(new SortedNumericDocValuesField(fieldName, v));
            json = "{ \"" + fieldName + "\" : \"" + (v == 0 ? "false" : "true") + "\" }";
        } else if (vst.equals(CoreValuesSourceType.IP)) {
            InetAddress ip = randomIp(randomBoolean());
            json = "{ \"" + fieldName + "\" : \"" + NetworkAddress.format(ip) + "\" }";
            doc.add(new SortedSetDocValuesField(fieldName, new BytesRef(InetAddressPoint.encode(ip))));
        } else if (vst.equals(CoreValuesSourceType.RANGE)) {
            Object start;
            Object end;
            RangeType rangeType;

            if (typeName.equals(RangeType.DOUBLE.typeName())) {
                start = randomDouble();
                end = RangeType.DOUBLE.nextUp(start);
                rangeType = RangeType.DOUBLE;
            } else if (typeName.equals(RangeType.FLOAT.typeName())) {
                start = randomFloat();
                end = RangeType.FLOAT.nextUp(start);
                rangeType = RangeType.DOUBLE;
            } else if (typeName.equals(RangeType.IP.typeName())) {
                boolean v4 = randomBoolean();
                start = randomIp(v4);
                end = RangeType.IP.nextUp(start);
                rangeType = RangeType.IP;
            } else if (typeName.equals(RangeType.LONG.typeName())) {
                start = randomLong();
                end = RangeType.LONG.nextUp(start);
                rangeType = RangeType.LONG;
            } else if (typeName.equals(RangeType.INTEGER.typeName())) {
                start = randomInt();
                end = RangeType.INTEGER.nextUp(start);
                rangeType = RangeType.INTEGER;
            } else if (typeName.equals(RangeType.DATE.typeName())) {
                start = randomNonNegativeLong();
                end = RangeType.DATE.nextUp(start);
                rangeType = RangeType.DATE;
            } else {
                throw new IllegalStateException("Unknown type of range [" + typeName + "]");
            }

            final RangeFieldMapper.Range range = new RangeFieldMapper.Range(rangeType, start, end, true, true);
            doc.add(new BinaryDocValuesField(fieldName, rangeType.encodeRanges(Collections.singleton(range))));
            json = "{ \""
                + fieldName
                + "\" : { \n"
                + "        \"gte\" : \""
                + start
                + "\",\n"
                + "        \"lte\" : \""
                + end
                + "\"\n"
                + "      }}";
        } else if (vst.equals(CoreValuesSourceType.GEOPOINT)) {
            double lat = randomDouble();
            double lon = randomDouble();
            doc.add(new LatLonDocValuesField(fieldName, lat, lon));
            json = "{ \"" + fieldName + "\" : \"[" + lon + "," + lat + "]\" }";
        } else {
            throw new IllegalStateException("Unknown field type [" + typeName + "]");
        }
        doc.add(new StoredField("_source", new BytesRef(json)));
        iw.addDocument(doc);
    }

    private class MockParserContext extends Mapper.TypeParser.ParserContext {
        MockParserContext() {
            super(null, null, null, Version.CURRENT, null, null, null);
        }

        @Override
        public Settings getSettings() {
            return Settings.EMPTY;
        }

        @Override
        public IndexAnalyzers getIndexAnalyzers() {
            NamedAnalyzer defaultAnalyzer = new NamedAnalyzer(
                AnalysisRegistry.DEFAULT_ANALYZER_NAME,
                AnalyzerScope.GLOBAL,
                new StandardAnalyzer()
            );
            return new IndexAnalyzers(singletonMap(AnalysisRegistry.DEFAULT_ANALYZER_NAME, defaultAnalyzer), emptyMap(), emptyMap());
        }
    }

    @After
    private void cleanupReleasables() {
        Releasables.close(releasables);
        releasables.clear();
    }

    /**
     * Hook for checking things after all {@link Aggregator}s have been closed.
     */
    protected void afterClose() {}

    /**
     * Make a {@linkplain DateFieldMapper.DateFieldType} for a {@code date}.
     */
    protected DateFieldMapper.DateFieldType dateField(String name, DateFieldMapper.Resolution resolution) {
        return new DateFieldMapper.DateFieldType(name, resolution);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code double}.
     */
    protected NumberFieldMapper.NumberFieldType doubleField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.DOUBLE);
    }

    /**
     * Make a {@linkplain GeoPointFieldMapper.GeoPointFieldType} for a {@code geo_point}.
     */
    protected GeoPointFieldMapper.GeoPointFieldType geoPointField(String name) {
        return new GeoPointFieldMapper.GeoPointFieldType(name);
    }

    /**
     * Make a {@linkplain DateFieldMapper.DateFieldType} for a {@code date}.
     */
    protected KeywordFieldMapper.KeywordFieldType keywordField(String name) {
        return new KeywordFieldMapper.KeywordFieldType(name);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code long}.
     */
    protected NumberFieldMapper.NumberFieldType longField(String name) {
        return new NumberFieldMapper.NumberFieldType(name, NumberFieldMapper.NumberType.LONG);
    }

    /**
     * Make a {@linkplain NumberFieldMapper.NumberFieldType} for a {@code range}.
     */
    protected RangeFieldMapper.RangeFieldType rangeField(String name, RangeType rangeType) {
        if (rangeType == RangeType.DATE) {
            return new RangeFieldMapper.RangeFieldType(name, RangeFieldMapper.Defaults.DATE_FORMATTER);
        }
        return new RangeFieldMapper.RangeFieldType(name, rangeType);
    }

    /**
     * Request an aggregation that returns the {@link CardinalityUpperBound}
     * that was passed to its ctor.
     */
    public static AggregationBuilder aggCardinality(String name) {
        return new AggCardinalityAggregationBuilder(name);
    }

    private static class AggCardinalityAggregationBuilder extends AbstractAggregationBuilder<AggCardinalityAggregationBuilder> {

        AggCardinalityAggregationBuilder(String name) {
            super(name);
        }

        @Override
        protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subfactoriesBuilder)
            throws IOException {
            return new AggregatorFactory(name, queryShardContext, parent, subfactoriesBuilder, metadata) {
                @Override
                protected Aggregator createInternal(
                    SearchContext searchContext,
                    Aggregator parent,
                    CardinalityUpperBound cardinality,
                    Map<String, Object> metadata
                ) throws IOException {
                    return new MetricsAggregator(name, searchContext, parent, metadata) {
                        @Override
                        protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
                            return LeafBucketCollector.NO_OP_COLLECTOR;
                        }

                        @Override
                        public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
                            return new InternalAggCardinality(name, cardinality, metadata);
                        }

                        @Override
                        public InternalAggregation buildEmptyAggregation() {
                            // TODO Auto-generated method stub
                            return null;
                        }
                    };
                }
            };
        }

        @Override
        protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }

        @Override
        public BucketCardinality bucketCardinality() {
            return BucketCardinality.ONE;
        }

        @Override
        public String getType() {
            return "agg_cardinality";
        }

        @Override
        protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();

        }
    }

    public static class InternalAggCardinality extends InternalAggregation {
        private final CardinalityUpperBound cardinality;

        protected InternalAggCardinality(String name, CardinalityUpperBound cardinality, Map<String, Object> metadata) {
            super(name, metadata);
            this.cardinality = cardinality;
        }

        public CardinalityUpperBound cardinality() {
            return cardinality;
        }

        @Override
        public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
            aggregations.forEach(ia -> { assertThat(((InternalAggCardinality) ia).cardinality, equalTo(cardinality)); });
            return new InternalAggCardinality(name, cardinality, metadata);
        }

        @Override
        protected boolean mustReduceOnSingleInternalAgg() {
            return true;
        }

        @Override
        public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
            return builder.array("cardinality", cardinality);
        }

        @Override
        public Object getProperty(List<String> path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }
    }

    private static class AggCardinalityPlugin implements SearchPlugin {
        @Override
        public List<AggregationSpec> getAggregations() {
            return singletonList(
                new AggregationSpec("agg_cardinality", in -> null, (ContextParser<String, AggCardinalityAggregationBuilder>) (p, c) -> null)
            );
        }
    }
}
