/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.search.pruning.SearchIndexPruningSettings;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.fielddomain.DateRangeFieldDomain;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numDataNodes = 1, numClientNodes = 0, supportsDedicatedMasters = false)
public class SearchIndexPruningIT extends OpenSearchIntegTestCase {
    public static class RecordOldIndexSearchPlugin extends Plugin implements NetworkPlugin {
        private static final AtomicBoolean RECORD_SEARCH_EVENTS = new AtomicBoolean();
        private static final AtomicInteger OLD_INDEX_QUERY_PHASES = new AtomicInteger();
        private static final AtomicInteger OVERLAPPING_INDEX_QUERY_PHASES = new AtomicInteger();
        private static final AtomicInteger CAN_MATCH_REQUESTS = new AtomicInteger();
        private static final AtomicInteger OLD_INDEX_CAN_MATCH_REQUESTS = new AtomicInteger();

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    if (RECORD_SEARCH_EVENTS.get() == false) {
                        return;
                    }
                    String indexName = searchContext.indexShard().shardId().getIndex().getName();
                    if ("logs-000001".equals(indexName)) {
                        OLD_INDEX_QUERY_PHASES.incrementAndGet();
                    } else if ("logs-000002".equals(indexName) || "logs-000003".equals(indexName)) {
                        OVERLAPPING_INDEX_QUERY_PHASES.incrementAndGet();
                    }
                }
            });
        }

        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return List.of(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (SearchTransportService.QUERY_CAN_MATCH_NAME.equals(action) == false) {
                        return actualHandler;
                    }
                    return new TransportRequestHandler<T>() {
                        @Override
                        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
                            recordCanMatchRequest(request);
                            actualHandler.messageReceived(request, channel, task);
                        }
                    };
                }
            });
        }

        private static void recordCanMatchRequest(TransportRequest request) {
            if (RECORD_SEARCH_EVENTS.get() && request instanceof ShardSearchRequest) {
                ShardSearchRequest shardSearchRequest = (ShardSearchRequest) request;
                CAN_MATCH_REQUESTS.incrementAndGet();
                if ("logs-000001".equals(shardSearchRequest.shardId().getIndexName())) {
                    OLD_INDEX_CAN_MATCH_REQUESTS.incrementAndGet();
                }
            }
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(RecordOldIndexSearchPlugin.class);
    }

    @Override
    public void tearDown() throws Exception {
        try {
            RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(false);
            RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.set(0);
            RecordOldIndexSearchPlugin.OVERLAPPING_INDEX_QUERY_PHASES.set(0);
            RecordOldIndexSearchPlugin.CAN_MATCH_REQUESTS.set(0);
            RecordOldIndexSearchPlugin.OLD_INDEX_CAN_MATCH_REQUESTS.set(0);
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("search.index_pruning.*"))
                .get();
        } finally {
            super.tearDown();
        }
    }

    public void testSearchUsesIndexFieldDomainMetadataToSkipShardGroups() throws Exception {
        SearchResponse response = searchWithIndexFieldDomainMetadata(1_000);

        assertHitCount(response, 1L);
        assertThat(RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.get(), equalTo(0));
    }

    public void testSearchUsesIndexFieldDomainMetadataBeforeCanMatch() throws Exception {
        SearchResponse response = searchWithIndexFieldDomainMetadata(1);

        assertHitCount(response, 1L);
        assertThat(RecordOldIndexSearchPlugin.OLD_INDEX_CAN_MATCH_REQUESTS.get(), equalTo(0));
        assertThat(RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.get(), equalTo(0));
    }

    public void testPointInTimeSearchDoesNotUseIndexFieldDomainMetadata() throws Exception {
        enableIndexPruning();
        createLogsIndices();

        index("logs-000001", "_doc", "1", "@timestamp", "1970-01-01T00:00:03.500Z", "message", "old");
        index("logs-000002", "_doc", "1", "@timestamp", "1970-01-01T00:00:10Z", "message", "current");
        refresh("logs-000001", "logs-000002");

        CreatePitRequest createPitRequest = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        createPitRequest.setIndices(new String[] { "logs-*" });
        CreatePitResponse pitResponse = client().execute(CreatePitAction.INSTANCE, createPitRequest).actionGet();

        try {
            publishFieldDomain("logs-000001", new DateRangeFieldDomain("@timestamp", 1_000L, 2_000L, true, "test"));
            publishFieldDomain("logs-000002", new DateRangeFieldDomain("@timestamp", 3_000L, 4_000L, true, "test"));

            resetSearchEventCounters();
            RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(true);
            SearchResponse response;
            try {
                response = client().prepareSearch("logs-*")
                    .setPreFilterShardSize(1_000)
                    .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
                    .setQuery(rangeQuery("@timestamp").gte("1970-01-01T00:00:03Z").lte("1970-01-01T00:00:04Z"))
                    .get();
            } finally {
                RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(false);
            }

            assertHitCount(response, 1L);
            assertThat(RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.get(), greaterThan(0));
        } finally {
            client().execute(DeletePitAction.INSTANCE, new DeletePitRequest(pitResponse.getId())).actionGet();
        }
    }

    public void testSearchKeepsIndicesWithDomainsPartiallyOverlappingQueryRange() throws Exception {
        enableIndexPruning();
        createLogsIndices();

        index("logs-000001", "_doc", "1", "@timestamp", "1970-01-01T00:00:01Z", "message", "old");
        index("logs-000002", "_doc", "1", "@timestamp", "1970-01-01T00:00:03.500Z", "message", "partial-left");
        index("logs-000003", "_doc", "1", "@timestamp", "1970-01-01T00:00:05Z", "message", "partial-right");
        refresh("logs-000001", "logs-000002", "logs-000003");

        publishFieldDomain("logs-000001", new DateRangeFieldDomain("@timestamp", 1_000L, 2_000L, true, "test"));
        publishFieldDomain("logs-000002", new DateRangeFieldDomain("@timestamp", 2_000L, 4_000L, true, "test"));
        publishFieldDomain("logs-000003", new DateRangeFieldDomain("@timestamp", 4_000L, 6_000L, true, "test"));

        resetSearchEventCounters();
        RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(true);
        SearchResponse response;
        try {
            response = client().prepareSearch("logs-*")
                .setPreFilterShardSize(1)
                .setQuery(rangeQuery("@timestamp").gte("1970-01-01T00:00:03Z").lte("1970-01-01T00:00:06Z"))
                .addSort("@timestamp", SortOrder.ASC)
                .get();
        } finally {
            RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(false);
        }

        assertHitCount(response, 2L);
        assertThat(RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.get(), equalTo(0));
        assertThat(RecordOldIndexSearchPlugin.OVERLAPPING_INDEX_QUERY_PHASES.get(), greaterThan(0));
    }

    private SearchResponse searchWithIndexFieldDomainMetadata(int preFilterShardSize) throws Exception {
        enableIndexPruning();
        createLogsIndices();

        index("logs-000001", "_doc", "1", "@timestamp", "1970-01-01T00:00:01Z", "message", "old");
        index("logs-000002", "_doc", "1", "@timestamp", "1970-01-01T00:00:03.500Z", "message", "current");
        refresh("logs-000001", "logs-000002");

        publishFieldDomain("logs-000001", new DateRangeFieldDomain("@timestamp", 1_000L, 2_000L, true, "test"));
        publishFieldDomain("logs-000002", new DateRangeFieldDomain("@timestamp", 3_000L, 4_000L, true, "test"));

        resetSearchEventCounters();
        RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(true);
        try {
            return client().prepareSearch("logs-*")
                .setPreFilterShardSize(preFilterShardSize)
                .setQuery(rangeQuery("@timestamp").gte("1970-01-01T00:00:03Z").lte("1970-01-01T00:00:04Z"))
                .addSort("@timestamp", SortOrder.ASC)
                .get();
        } finally {
            RecordOldIndexSearchPlugin.RECORD_SEARCH_EVENTS.set(false);
        }
    }

    private void enableIndexPruning() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(SearchIndexPruningSettings.ENABLED.getKey(), true)
                        .put(SearchIndexPruningSettings.MIN_SHARDS.getKey(), 1)
                        .putList(SearchIndexPruningSettings.FIELDS.getKey(), "@timestamp")
                )
                .get()
        );
    }

    private void createLogsIndices() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();
        assertAcked(prepareCreate("logs-000001").setSettings(indexSettings).setMapping("@timestamp", "type=date"));
        assertAcked(prepareCreate("logs-000002").setSettings(indexSettings).setMapping("@timestamp", "type=date"));
        assertAcked(prepareCreate("logs-000003").setSettings(indexSettings).setMapping("@timestamp", "type=date"));
        ensureGreen("logs-000001", "logs-000002", "logs-000003");
    }

    private void resetSearchEventCounters() {
        RecordOldIndexSearchPlugin.OLD_INDEX_QUERY_PHASES.set(0);
        RecordOldIndexSearchPlugin.OVERLAPPING_INDEX_QUERY_PHASES.set(0);
        RecordOldIndexSearchPlugin.CAN_MATCH_REQUESTS.set(0);
        RecordOldIndexSearchPlugin.OLD_INDEX_CAN_MATCH_REQUESTS.set(0);
    }

    private void publishFieldDomain(String index, DateRangeFieldDomain domain) throws Exception {
        ClusterService clusterService = internalCluster().getClusterManagerNodeInstance(ClusterService.class);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> failure = new AtomicReference<>();

        clusterService.submitStateUpdateTask("put-index-field-domain [" + index + "]", new ClusterStateUpdateTask(Priority.URGENT) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                IndexMetadata current = currentState.metadata().index(index);
                if (current == null) {
                    failure.set(new IllegalStateException("index [" + index + "] not found in cluster state"));
                    return currentState;
                }

                IndexMetadata updated = IndexFieldDomainMetadata.getInstance().putFieldDomain(current, domain);
                Metadata metadata = Metadata.builder(currentState.metadata()).put(updated, true).build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                failure.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw failure.get();
        }
    }
}
