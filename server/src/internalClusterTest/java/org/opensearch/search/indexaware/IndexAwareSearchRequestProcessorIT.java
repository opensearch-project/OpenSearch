/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.PutSearchPipelineRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Runs {@link IndexAwareSearchRequestProcessor}s registered by test plugins through real searches on a
 * test cluster.
 * <p>
 * Every test searches one or both of two indices with the same three documents and a {@code title} text
 * field. They differ in one thing: {@link #OPTED_IN_INDEX} marks {@code title} in its mapping (through the
 * field's {@code meta} parameter), {@link #OPTED_OUT_INDEX} does not. The main test processor
 * ({@link TestIndexAwareSearchRequestProcessorPlugin}) turns a {@code match} on {@code title} into a
 * {@code match_all} when the target index is marked. A search for "shoes" therefore returns 1 hit when the
 * processor left it alone and 3 hits when the processor changed it, which is how the tests tell the two
 * apart.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class IndexAwareSearchRequestProcessorIT extends OpenSearchIntegTestCase {

    static final String OPTED_IN_INDEX = "opted_in_index";
    static final String OPTED_OUT_INDEX = "opted_out_index";

    /** Hits for "shoes" when the query ran as the caller wrote it. */
    private static final long UNCHANGED = 1;
    /** Hits for "shoes" when a processor turned the query into {@code match_all}. */
    private static final long REWRITTEN = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Processors run in this order.
        return Arrays.asList(
            TestIndexAwareSearchRequestProcessorPlugin.class,
            NestedSearchTriggeringPlugin.class,
            MarkerObservingPlugin.class,
            FailingOnTriggerPlugin.class,
            BeforePipelineRewritingPlugin.class
        );
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createIndexWithTitleField(client(), OPTED_IN_INDEX, true);
        createIndexWithTitleField(client(), OPTED_OUT_INDEX, false);
    }

    /**
     * Creates {@code index} with a {@code title} text field and three documents, one of which contains
     * "shoes". With {@code optIn}, the field's mapping carries the marker the test processor looks for.
     */
    static void createIndexWithTitleField(Client client, String index, boolean optIn) throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("properties").startObject("title").field("type", "text");
        if (optIn) {
            mapping.startObject("meta").field(TestIndexAwareSearchRequestProcessorPlugin.OPT_IN_META_KEY, "true").endObject();
        }
        mapping.endObject().endObject().endObject();
        client.admin().indices().prepareCreate(index).setMapping(mapping).get();
        client.prepareIndex(index).setId("1").setSource("title", "red shoes").get();
        client.prepareIndex(index).setId("2").setSource("title", "blue jacket").get();
        client.prepareIndex(index).setId("3").setSource("title", "green hat").get();
        client.admin().indices().prepareRefresh(index).get();
    }

    // --- the setting, and the per-index decision ---

    // The plugin is installed and the mapping opts in, but the setting is off by default.
    public void testProcessorsDoNotRunWhileTheSettingIsOff() {
        assertHitCount(searchForShoes(OPTED_IN_INDEX), UNCHANGED);
    }

    public void testOptedInIndexIsRewritten() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(searchForShoes(OPTED_IN_INDEX), REWRITTEN));
    }

    // The processor ran, read this index's mapping and found no marker: the decision is per index.
    public void testIndexThatDidNotOptInIsNotRewritten() throws Exception {
        withProcessorsEnabled(() -> assertHitCount(searchForShoes(OPTED_OUT_INDEX), UNCHANGED));
    }

    // --- entry points ---

    // Processors are not part of a search pipeline, so asking for no pipeline does not switch them off.
    public void testSearchPipelineNoneDoesNotBypassProcessors() throws Exception {
        withProcessorsEnabled(() -> {
            SearchRequest searchRequest = new SearchRequest(OPTED_IN_INDEX).source(new SearchSourceBuilder().query(shoesQuery()));
            searchRequest.pipeline("_none");
            assertHitCount(client().search(searchRequest).actionGet(), REWRITTEN);
        });
    }

    // Each item of a multi-search is a search of its own, processed for its own target indices.
    public void testEachMultiSearchItemIsProcessedForItsOwnIndices() throws Exception {
        withProcessorsEnabled(() -> {
            MultiSearchResponse response = client().prepareMultiSearch()
                .add(client().prepareSearch(OPTED_IN_INDEX).setQuery(shoesQuery()))
                .add(client().prepareSearch(OPTED_OUT_INDEX).setQuery(shoesQuery()))
                .get();
            assertHitCount(response.getResponses()[0].getResponse(), REWRITTEN);
            assertHitCount(response.getResponses()[1].getResponse(), UNCHANGED);
        });
    }

    // A count is a search with size 0. It is not an internal search: processors apply to it, so a count
    // agrees with the search it counts.
    public void testSizeZeroSearchIsProcessedLikeAnyOtherSearch() throws Exception {
        withProcessorsEnabled(() -> {
            SearchResponse response = client().prepareSearch(OPTED_IN_INDEX)
                .setQuery(shoesQuery())
                .setSize(0)
                .setTrackTotalHits(true)
                .get();
            assertHitCount(response, REWRITTEN);
        });
    }

    // Creating a point in time opens shard contexts but runs no query, so there is nothing to process.
    public void testCreatingAPointInTimeDoesNotRunProcessors() throws Exception {
        MarkerObservingPlugin.OBSERVATIONS.set(0);
        withProcessorsEnabled(() -> {
            CreatePitResponse pit = createPointInTime(OPTED_IN_INDEX);
            try {
                assertEquals(0, MarkerObservingPlugin.OBSERVATIONS.get());
            } finally {
                client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pit.getId())).get();
            }
        });
    }

    // A point-in-time search names no index: its indices are recorded in the PIT, and that is where the
    // processors' context has to take them from.
    public void testPointInTimeSearchIsRewrittenPerItsIndexMapping() throws Exception {
        CreatePitResponse pit = createPointInTime(OPTED_IN_INDEX);
        try {
            withProcessorsEnabled(() -> {
                SearchResponse response = client().prepareSearch()
                    .setPointInTime(new PointInTimeBuilder(pit.getId()))
                    .setQuery(shoesQuery())
                    .get();
                assertHitCount(response, REWRITTEN);
            });
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pit.getId())).get();
        }
    }

    // --- sub-searches a processor issues ---

    // NestedSearchTriggeringPlugin's processor issues a marked sub-search of the same query. Without the
    // marker, that sub-search would reach the processor again. The search targets OPTED_OUT_INDEX so that
    // the first processor in the chain leaves the trigger query alone.
    public void testProcessorIsNotInvokedAgainForTheSubSearchItIssues() throws Exception {
        NestedSearchTriggeringPlugin.reset();
        withProcessorsEnabled(() -> {
            client().prepareSearch(OPTED_OUT_INDEX).setQuery(QueryBuilders.matchQuery("title", NestedSearchTriggeringPlugin.TRIGGER)).get();
            assertTrue(NestedSearchTriggeringPlugin.ISSUED_NESTED_SEARCH.get());
            assertEquals(1, NestedSearchTriggeringPlugin.INVOCATIONS.get());
        });
    }

    // The same processor completes from its sub-search's callback, which runs in the sub-search's thread
    // context: marked. The processor after it (MarkerObservingPlugin) and the rest of the search must run
    // in the request's own context, or the marker meant for one sub-search would cover the whole search.
    public void testSubSearchMarkerDoesNotLeakIntoTheRestOfTheSearch() throws Exception {
        NestedSearchTriggeringPlugin.reset();
        MarkerObservingPlugin.OBSERVATIONS.set(0);
        MarkerObservingPlugin.SAW_MARKER.set(false);
        withProcessorsEnabled(() -> {
            SearchResponse response = client().prepareSearch(OPTED_OUT_INDEX)
                .setQuery(QueryBuilders.matchQuery("title", NestedSearchTriggeringPlugin.TRIGGER))
                .get();
            assertHitCount(response, 0);
            // The processor really completed from the sub-search's callback, not inline.
            assertNotNull(NestedSearchTriggeringPlugin.COMPLETED_ON_THREAD.get());
            assertNotEquals(Thread.currentThread().getName(), NestedSearchTriggeringPlugin.COMPLETED_ON_THREAD.get());
            // It ran once, for the outer search only, and not under the marker.
            assertEquals(1, MarkerObservingPlugin.OBSERVATIONS.get());
            assertFalse("a later processor ran under the sub-search marker", MarkerObservingPlugin.SAW_MARKER.get());
        });
    }

    // Collapsing with inner hits makes the search issue one more search per group, built around the query
    // the processors already produced. Those searches are part of the caller's search, not new requests:
    // processing them would apply the processors to their own output.
    public void testCollapseExpansionSearchesAreNotProcessedAgain() throws Exception {
        String index = "collapse_index";
        client().admin().indices().prepareCreate(index).setMapping("title", "type=text", "group", "type=keyword").get();
        client().prepareIndex(index).setId("1").setSource("title", "red shoes", "group", "a").get();
        client().prepareIndex(index).setId("2").setSource("title", "blue shoes", "group", "a").get();
        client().prepareIndex(index).setId("3").setSource("title", "green shoes", "group", "b").get();
        client().admin().indices().prepareRefresh(index).get();
        MarkerObservingPlugin.OBSERVATIONS.set(0);
        withProcessorsEnabled(() -> {
            SearchResponse response = client().prepareSearch(index)
                .setQuery(shoesQuery())
                .setCollapse(new CollapseBuilder("group").setInnerHits(new InnerHitBuilder("by_group")))
                .get();
            // Two groups, each expanded by a search of its own.
            assertEquals(2, response.getHits().getHits().length);
            for (SearchHit hit : response.getHits().getHits()) {
                assertNotNull(hit.getInnerHits().get("by_group"));
            }
            // One invocation, for the search the caller sent; none for the two expansion searches.
            assertEquals(1, MarkerObservingPlugin.OBSERVATIONS.get());
        });
    }

    // --- stages ---

    // A BEFORE_PIPELINE processor runs ahead of the request's search pipeline: the pipeline's own request
    // processor, an ordinary SearchRequestProcessor, sees the query the index-aware processor produced.
    public void testBeforePipelineProcessorRunsAheadOfTheSearchPipeline() throws Exception {
        withQueryRecordingPipeline(pipelineId -> {
            SearchRequest searchRequest = new SearchRequest(OPTED_OUT_INDEX).source(
                new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", BeforePipelineRewritingPlugin.TRIGGER))
            );
            searchRequest.pipeline(pipelineId);

            SearchResponse response = client().search(searchRequest).actionGet();

            assertThat(BeforePipelineRewritingPlugin.SEEN_BY_PIPELINE.get(), instanceOf(MatchAllQueryBuilder.class));
            assertHitCount(response, REWRITTEN);
        });
    }

    // The default stage is the other way round: the pipeline's request processor sees the query the
    // caller wrote, and the index-aware processor rewrites it afterwards.
    public void testAfterPipelineProcessorRunsBehindTheSearchPipeline() throws Exception {
        withQueryRecordingPipeline(pipelineId -> {
            SearchRequest searchRequest = new SearchRequest(OPTED_IN_INDEX).source(new SearchSourceBuilder().query(shoesQuery()));
            searchRequest.pipeline(pipelineId);

            SearchResponse response = client().search(searchRequest).actionGet();

            assertThat(BeforePipelineRewritingPlugin.SEEN_BY_PIPELINE.get(), instanceOf(MatchQueryBuilder.class));
            assertHitCount(response, REWRITTEN);
        });
    }

    // --- failures and side effects ---

    // A processor failure fails the search with the processor's exception, rather than running the
    // query without the processor.
    public void testFailingProcessorFailsTheSearch() throws Exception {
        withProcessorsEnabled(() -> {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> client().prepareSearch(OPTED_OUT_INDEX)
                    .setQuery(QueryBuilders.matchQuery("title", FailingOnTriggerPlugin.TRIGGER))
                    .get()
            );
            assertThat(e.getMessage(), containsString(FailingOnTriggerPlugin.MESSAGE));
        });
    }

    // The test processor sets a new query on the source it is given. The caller's own SearchRequest must
    // still hold the query the caller built, so that the request can be sent again.
    public void testCallersSearchRequestIsLeftAsItWas() throws Exception {
        withProcessorsEnabled(() -> {
            QueryBuilder callersQuery = shoesQuery();
            SearchRequest searchRequest = new SearchRequest(OPTED_IN_INDEX).source(new SearchSourceBuilder().query(callersQuery));

            assertHitCount(client().search(searchRequest).actionGet(), REWRITTEN);
            assertSame(callersQuery, searchRequest.source().query());
            // Sending it again gives the same answer.
            assertHitCount(client().search(searchRequest).actionGet(), REWRITTEN);
        });
    }

    public void testDeletingAnIndexEvictsItsCachedMapping() throws Exception {
        String index = "evicted_index";
        createIndexWithTitleField(client(), index, true);
        withProcessorsEnabled(() -> {
            int before = cachedMappings();
            // The test processor reads the index's mapping through the context, which parses and caches
            // it on the node that coordinates the search.
            assertHitCount(searchForShoes(index), REWRITTEN);
            assertEquals(before + 1, cachedMappings());

            client().admin().indices().prepareDelete(index).get();
            // A cluster state listener on the coordinating node removes the entry, which may happen
            // after the delete call returns.
            assertBusy(() -> assertEquals(before, cachedMappings()));
        });
    }

    // --- helpers ---

    private static QueryBuilder shoesQuery() {
        return QueryBuilders.matchQuery("title", "shoes");
    }

    private SearchResponse searchForShoes(String... indices) {
        return client().prepareSearch(indices).setQuery(shoesQuery()).get();
    }

    private CreatePitResponse createPointInTime(String index) throws Exception {
        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueMinutes(1), true);
        createPitRequest.setIndices(new String[] { index });
        return client().execute(CreatePitAction.INSTANCE, createPitRequest).get();
    }

    /** Parsed mappings cached on any node: whichever one coordinated a search holds the entry. */
    private static int cachedMappings() {
        int total = 0;
        for (SearchService searchService : internalCluster().getInstances(SearchService.class)) {
            total += searchService.getIndexMappingSourceCache().size();
        }
        return total;
    }

    /**
     * Runs {@code test} with the processors switched on and a search pipeline in place whose one request
     * processor records the query it is given in {@link BeforePipelineRewritingPlugin#SEEN_BY_PIPELINE}.
     */
    private void withQueryRecordingPipeline(CheckedConsumer<String, Exception> test) throws Exception {
        String pipelineId = "record-query-pipeline";
        BytesArray pipeline = new BytesArray(
            "{ \"request_processors\": [ { \"" + BeforePipelineRewritingPlugin.RECORD_QUERY_TYPE + "\": {} } ] }"
        );
        client().admin()
            .cluster()
            .putSearchPipeline(new PutSearchPipelineRequest(pipelineId, pipeline, MediaTypeRegistry.JSON))
            .actionGet();
        BeforePipelineRewritingPlugin.SEEN_BY_PIPELINE.set(null);
        try {
            withProcessorsEnabled(() -> test.accept(pipelineId));
        } finally {
            client().admin().cluster().deleteSearchPipeline(new DeleteSearchPipelineRequest(pipelineId)).actionGet();
        }
    }

    /** Runs {@code test} with the processors switched on, and switches them off again whatever happens. */
    private void withProcessorsEnabled(CheckedRunnable<Exception> test) throws Exception {
        setProcessorsEnabled(client(), true);
        try {
            test.run();
        } finally {
            setProcessorsEnabled(client(), null);
        }
    }

    /** Sets the setting that enables the processors as a transient cluster setting; {@code null} removes it. */
    static void setProcessorsEnabled(Client client, Boolean enabled) {
        String key = SearchService.INDEX_AWARE_SEARCH_REQUEST_PROCESSORS_ENABLED_SETTING.getKey();
        Settings.Builder settings = enabled == null ? Settings.builder().putNull(key) : Settings.builder().put(key, enabled);
        client.admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get();
    }

    // --- test plugins ---

    /**
     * Turns a {@code match} on {@code title} into {@code match_all} if a target index marks the field in
     * its mapping, with {@code "meta": {"index_aware_test_rewrite": "true"}}. This is the simplest
     * processor that depends on the mapping of the index being searched.
     */
    public static class TestIndexAwareSearchRequestProcessorPlugin extends Plugin implements SearchPlugin {

        static final String OPT_IN_META_KEY = "index_aware_test_rewrite";

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                if (source.query() instanceof MatchQueryBuilder matchQuery && "title".equals(matchQuery.fieldName())) {
                    for (String indexName : context.getTargetIndices().keySet()) {
                        if (isOptedIn(context, indexName)) {
                            listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                            return;
                        }
                    }
                }
                listener.onResponse(source);
            });
        }

        /** Whether {@code indexName}'s mapping marks the {@code title} field. */
        static boolean isOptedIn(IndexAwareContext context, String indexName) {
            Object properties = context.getMappingSourceAsMap(indexName).get("properties");
            if (properties instanceof Map<?, ?> propertiesMap && propertiesMap.get("title") instanceof Map<?, ?> titleMapping) {
                Object meta = titleMapping.get("meta");
                return meta instanceof Map<?, ?> metaMap && "true".equals(metaMap.get(OPT_IN_META_KEY));
            }
            return false;
        }
    }

    /**
     * A processor that, on seeing a {@code match} on {@code title} for {@link #TRIGGER}, issues a marked
     * sub-search of that same query against the same index, and completes from the sub-search's callback.
     * It issues at most one sub-search per {@link #reset()}, so a marker that did not work shows up as a
     * second invocation, not as endless recursion.
     */
    public static class NestedSearchTriggeringPlugin extends Plugin implements SearchPlugin {

        static final String TRIGGER = "nested_trigger";
        static final AtomicInteger INVOCATIONS = new AtomicInteger();
        static final AtomicBoolean ISSUED_NESTED_SEARCH = new AtomicBoolean();
        static final AtomicReference<String> COMPLETED_ON_THREAD = new AtomicReference<>();

        // Per plugin instance, that is per node: a test cluster runs several nodes in one JVM, each with
        // its own ThreadContext, and a processor has to use the one of the node it runs on.
        private volatile Client nodeClient;
        private volatile ThreadPool nodeThreadPool;

        static void reset() {
            INVOCATIONS.set(0);
            ISSUED_NESTED_SEARCH.set(false);
            COMPLETED_ON_THREAD.set(null);
        }

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            nodeClient = client;
            nodeThreadPool = threadPool;
            return Collections.emptyList();
        }

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                INVOCATIONS.incrementAndGet();
                if (isTrigger(source) && ISSUED_NESTED_SEARCH.compareAndSet(false, true)) {
                    String indexName = context.getTargetIndices().keySet().iterator().next();
                    SearchRequest subSearch = new SearchRequest(indexName).source(
                        new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", TRIGGER))
                    );
                    // Cancelled with, and accounted to, the search that caused it.
                    subSearch.setParentTask(context.getSearchTaskId());
                    // Mark only the call that issues the sub-search, do not wait for it, and complete the
                    // processor's listener from the sub-search's own callback.
                    try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(nodeThreadPool.getThreadContext())) {
                        nodeClient.search(subSearch, ActionListener.wrap(response -> {
                            COMPLETED_ON_THREAD.set(Thread.currentThread().getName());
                            listener.onResponse(source);
                        }, listener::onFailure));
                    }
                } else {
                    listener.onResponse(source);
                }
            });
        }

        private static boolean isTrigger(SearchSourceBuilder source) {
            return source.query() instanceof MatchQueryBuilder matchQuery
                && "title".equals(matchQuery.fieldName())
                && TRIGGER.equals(matchQuery.value());
        }
    }

    /**
     * A processor that counts every invocation and records whether the sub-search marker was set when it
     * ran. It is registered after {@link NestedSearchTriggeringPlugin}, so it runs once that processor has
     * completed from its sub-search's callback.
     */
    public static class MarkerObservingPlugin extends Plugin implements SearchPlugin {

        static final AtomicInteger OBSERVATIONS = new AtomicInteger();
        static final AtomicBoolean SAW_MARKER = new AtomicBoolean();

        private volatile ThreadPool nodeThreadPool;

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            nodeThreadPool = threadPool;
            return Collections.emptyList();
        }

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                OBSERVATIONS.incrementAndGet();
                if (SubSearchMarker.isMarked(nodeThreadPool.getThreadContext())) {
                    SAW_MARKER.set(true);
                }
                listener.onResponse(source);
            });
        }
    }

    /** A processor that fails its listener on seeing a {@code match} on {@code title} for {@link #TRIGGER}. */
    public static class FailingOnTriggerPlugin extends Plugin implements SearchPlugin {

        static final String TRIGGER = "failing_trigger";
        static final String MESSAGE = "the test processor failed on purpose";

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList((source, context, listener) -> {
                if (source.query() instanceof MatchQueryBuilder matchQuery && TRIGGER.equals(matchQuery.value())) {
                    listener.onFailure(new IllegalStateException(MESSAGE));
                } else {
                    listener.onResponse(source);
                }
            });
        }
    }

    /**
     * Registers two things that together show where a {@code BEFORE_PIPELINE} processor runs:
     * <ul>
     *   <li>an index-aware processor at {@code BEFORE_PIPELINE} that turns a {@code match} on {@code title}
     *       for {@link #TRIGGER} into {@code match_all};</li>
     *   <li>a search pipeline request processor type, {@link #RECORD_QUERY_TYPE}, that records the query
     *       it is given.</li>
     * </ul>
     */
    public static class BeforePipelineRewritingPlugin extends Plugin implements SearchPlugin, SearchPipelinePlugin {

        static final String TRIGGER = "before_pipeline_trigger";
        static final String RECORD_QUERY_TYPE = "record_query";
        static final AtomicReference<QueryBuilder> SEEN_BY_PIPELINE = new AtomicReference<>();

        @Override
        public List<IndexAwareSearchRequestProcessor> getIndexAwareSearchRequestProcessors() {
            return singletonList(new IndexAwareSearchRequestProcessor() {
                @Override
                public void process(SearchSourceBuilder source, IndexAwareContext context, ActionListener<SearchSourceBuilder> listener) {
                    if (source.query() instanceof MatchQueryBuilder matchQuery
                        && "title".equals(matchQuery.fieldName())
                        && TRIGGER.equals(matchQuery.value())) {
                        listener.onResponse(source.query(QueryBuilders.matchAllQuery()));
                    } else {
                        listener.onResponse(source);
                    }
                }

                @Override
                public ExecutionStage getExecutionStage() {
                    return ExecutionStage.BEFORE_PIPELINE;
                }
            });
        }

        @Override
        public Map<String, Processor.Factory<SearchRequestProcessor>> getRequestProcessors(Parameters parameters) {
            return Map.of(
                RECORD_QUERY_TYPE,
                (processorFactories, tag, description, ignoreFailure, config, pipelineContext) -> new RecordQueryProcessor(
                    tag,
                    description,
                    ignoreFailure
                )
            );
        }
    }

    /** A search pipeline request processor that records the query of the request it is given. */
    private static final class RecordQueryProcessor extends AbstractProcessor implements SearchRequestProcessor {

        RecordQueryProcessor(String tag, String description, boolean ignoreFailure) {
            super(tag, description, ignoreFailure);
        }

        @Override
        public String getType() {
            return BeforePipelineRewritingPlugin.RECORD_QUERY_TYPE;
        }

        @Override
        public SearchRequest processRequest(SearchRequest request) {
            if (request.source() != null) {
                BeforePipelineRewritingPlugin.SEEN_BY_PIPELINE.set(request.source().query());
            }
            return request;
        }
    }
}
