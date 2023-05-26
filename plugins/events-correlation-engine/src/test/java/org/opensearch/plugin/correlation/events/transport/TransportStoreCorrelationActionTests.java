/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.transport;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.correlation.events.action.StoreCorrelationRequest;
import org.opensearch.plugin.correlation.events.action.StoreCorrelationResponse;
import org.opensearch.plugin.correlation.events.model.Correlation;
import org.opensearch.plugin.correlation.settings.EventsCorrelationSettings;
import org.opensearch.plugin.correlation.utils.CorrelationIndices;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportStoreCorrelationActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private Settings settings;
    private ClusterService clusterService;
    private CorrelationIndices correlationIndices;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportStoreCorrelationActionTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)
        );

        settings = Settings.builder()
            .put(EventsCorrelationSettings.CORRELATION_TIME_WINDOW.getKey(), new TimeValue(5, TimeUnit.MINUTES))
            .build();
        Set<Setting<?>> settingSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(EventsCorrelationSettings.CORRELATION_TIME_WINDOW);
        ClusterSettings clusterSettings = new ClusterSettings(settings, settingSet);
        clusterService = createClusterService(threadPool, discoveryNode, clusterSettings);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    @SuppressWarnings("unchecked")
    private void setupCorrelationIndices(boolean flag) throws IOException {
        correlationIndices = mock(CorrelationIndices.class);
        when(correlationIndices.correlationIndexExists()).thenReturn(flag);
        doAnswer((Answer<Object>) invocation -> {
            ((ActionListener<CreateIndexResponse>) invocation.getArgument(0)).onResponse(
                new CreateIndexResponse(true, true, Correlation.CORRELATION_HISTORY_INDEX)
            );
            return null;
        }).when(correlationIndices).initCorrelationIndex(any());
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                ((ActionListener<BulkResponse>) invocation.getArgument(1)).onResponse(new BulkResponse(new BulkItemResponse[] {}, 3, 4));
                return null;
            }
        }).when(correlationIndices).setupCorrelationIndex(anyLong(), any());
    }

    private TransportStoreCorrelationAction createActionForOrphanEvents(boolean indexListenerStatus) {
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
                SearchHit hit = new SearchHit(
                    1,
                    "1",
                    Map.of(
                        "@timestamp",
                        new DocumentField("@timestamp", List.of(200000L)),
                        "level",
                        new DocumentField("level", List.of(1L))
                    ),
                    null
                ).sourceRef(
                    new BytesArray(
                        "{\n"
                            + "  \"score_timestamp\": 200000,\n"
                            + "  \"event1\": \"correlated-event\",\n"
                            + "  \"index1\": \"correlated-index\",\n"
                            + "  \"event2\": \"correlated-event\",\n"
                            + "  \"index2\": \"correlated-index\",\n"
                            + "  \"level\": \"1\",\n"
                            + "  \"timestamp\": \"10000\"\n"
                            + "}"
                    )
                );
                hit.score(1.0f);
                listener.onResponse(
                    new SearchResponse(
                        new SearchResponseSections(
                            new SearchHits(new SearchHit[] { hit }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
                            null,
                            null,
                            false,
                            false,
                            null,
                            0
                        ),
                        null,
                        1,
                        1,
                        0,
                        20L,
                        null,
                        null
                    )
                );
            }

            @Override
            public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                listener.onResponse(new MultiSearchResponse(new MultiSearchResponse.Item[] {}, 10000L));
            }

            @Override
            public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
                IndexResponse response = new IndexResponse(
                    new ShardId(new Index("test-index", ""), 0),
                    "0",
                    0L,
                    0L,
                    0L,
                    indexListenerStatus
                );
                response.setShardInfo(new ReplicationResponse.ShardInfo());
                listener.onResponse(response);
            }
        };

        return new TransportStoreCorrelationAction(transportService, client, settings, actionFilters, correlationIndices, clusterService);
    }

    private TransportStoreCorrelationAction createActionForCorrelatedEvents(boolean indexListenerStatus) {
        CapturingTransport capturingTransport = new CapturingTransport();
        TransportService transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        ActionFilters actionFilters = new ActionFilters(new HashSet<>());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
                SearchHit hit = new SearchHit(
                    1,
                    "1",
                    Map.of(
                        "@timestamp",
                        new DocumentField("@timestamp", List.of(200000L)),
                        "level",
                        new DocumentField("level", List.of(1L))
                    ),
                    null
                ).sourceRef(
                    new BytesArray(
                        "{\n"
                            + "  \"score_timestamp\": 200000,\n"
                            + "  \"event1\": \"correlated-event\",\n"
                            + "  \"index1\": \"correlated-index\",\n"
                            + "  \"event2\": \"correlated-event\",\n"
                            + "  \"index2\": \"correlated-index\",\n"
                            + "  \"level\": \"1\",\n"
                            + "  \"timestamp\": \"10000\"\n"
                            + "}"
                    )
                );
                hit.score(1.0f);
                listener.onResponse(
                    new SearchResponse(
                        new SearchResponseSections(
                            new SearchHits(new SearchHit[] { hit }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
                            null,
                            null,
                            false,
                            false,
                            null,
                            0
                        ),
                        null,
                        1,
                        1,
                        0,
                        20L,
                        null,
                        null
                    )
                );
            }

            @Override
            public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                SearchHit hit = new SearchHit(
                    1,
                    "1",
                    Map.of(
                        "@timestamp",
                        new DocumentField("@timestamp", List.of(200000L)),
                        "level",
                        new DocumentField("level", List.of(1L))
                    ),
                    null
                ).sourceRef(
                    new BytesArray(
                        "{\n"
                            + "  \"score_timestamp\": 200000,\n"
                            + "  \"event1\": \"correlated-event\",\n"
                            + "  \"index1\": \"correlated-index\",\n"
                            + "  \"event2\": \"correlated-event\",\n"
                            + "  \"index2\": \"correlated-index\",\n"
                            + "  \"level\": \"1\",\n"
                            + "  \"timestamp\": \"10000\"\n"
                            + "}"
                    )
                );
                MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[] {
                    new MultiSearchResponse.Item(
                        new SearchResponse(
                            new SearchResponseSections(
                                new SearchHits(new SearchHit[] { hit }, new TotalHits(1L, TotalHits.Relation.EQUAL_TO), 1.0f),
                                null,
                                null,
                                false,
                                false,
                                null,
                                0
                            ),
                            null,
                            1,
                            1,
                            0,
                            20L,
                            null,
                            null
                        ),
                        null
                    ) };
                listener.onResponse(new MultiSearchResponse(items, 10000L));
            }

            @Override
            public void index(IndexRequest request, ActionListener<IndexResponse> listener) {
                IndexResponse response = new IndexResponse(
                    new ShardId(new Index("test-index", ""), 0),
                    "0",
                    0L,
                    0L,
                    0L,
                    indexListenerStatus
                );
                response.setShardInfo(new ReplicationResponse.ShardInfo());
                listener.onResponse(response);
            }

            @Override
            public void bulk(BulkRequest request, ActionListener<BulkResponse> listener) {
                listener.onResponse(new BulkResponse(new BulkItemResponse[] {}, 60000));
            }
        };

        return new TransportStoreCorrelationAction(transportService, client, settings, actionFilters, correlationIndices, clusterService);
    }

    public void testDoExecuteOrphanEvent() throws IOException {
        setupCorrelationIndices(true);
        TransportStoreCorrelationAction action = createActionForOrphanEvents(true);
        StoreCorrelationRequest request = new StoreCorrelationRequest(
            "test-index",
            "test-event",
            300000L,
            Map.of("network", List.of("correlated-event1", "correlated-event2")),
            List.of()
        );
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(StoreCorrelationResponse storeCorrelationResponse) {
                Assert.assertEquals(RestStatus.OK, storeCorrelationResponse.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }

    public void testDoExecuteOrphanEventWithSettingUpCorrelationIndex() throws IOException {
        setupCorrelationIndices(false);
        TransportStoreCorrelationAction action = createActionForOrphanEvents(true);
        StoreCorrelationRequest request = new StoreCorrelationRequest(
            "test-index",
            "test-event",
            300000L,
            Map.of("network", List.of("correlated-event1", "correlated-event2")),
            List.of()
        );
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(StoreCorrelationResponse storeCorrelationResponse) {
                Assert.assertEquals(RestStatus.OK, storeCorrelationResponse.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }

    public void testDoExecuteOrphanEventWithScoreTimestampReset() throws IOException {
        setupCorrelationIndices(true);
        TransportStoreCorrelationAction action = createActionForOrphanEvents(false);
        StoreCorrelationRequest request = new StoreCorrelationRequest(
            "test-index",
            "test-event",
            1728300000L,
            Map.of("network", List.of("correlated-event1", "correlated-event2")),
            List.of()
        );
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(StoreCorrelationResponse storeCorrelationResponse) {
                Assert.assertEquals(RestStatus.OK, storeCorrelationResponse.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }

    public void testExecuteCorrelatedEvents() throws IOException {
        setupCorrelationIndices(true);
        TransportStoreCorrelationAction action = createActionForCorrelatedEvents(false);
        StoreCorrelationRequest request = new StoreCorrelationRequest(
            "test-index",
            "test-event",
            1728300000L,
            Map.of("network", List.of("correlated-event1", "correlated-event2")),
            List.of()
        );
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(StoreCorrelationResponse storeCorrelationResponse) {
                Assert.assertEquals(RestStatus.OK, storeCorrelationResponse.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }
}
