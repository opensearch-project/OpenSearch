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
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.support.ActionFilters;
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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationRequest;
import org.opensearch.plugin.correlation.events.action.IndexCorrelationResponse;
import org.opensearch.plugin.correlation.settings.EventsCorrelationSettings;
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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class TransportIndexCorrelationActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private Settings settings;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportIndexCorrelationActionTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

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

    private TransportIndexCorrelationAction createAction() {
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
        NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(List.of());

        NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {

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
                );
                SearchResponse response = new SearchResponse(
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
                );
                MultiSearchResponse mSearchResponse = new MultiSearchResponse(
                    new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(response, null) },
                    1L
                );
                listener.onResponse(mSearchResponse);
            }

            @Override
            public void search(SearchRequest request, ActionListener<SearchResponse> listener) {
                SearchHit hit = new SearchHit(1).sourceRef(
                    new BytesArray(
                        "{\n"
                            + "  \"name\": \"s3 to app logs\",\n"
                            + "  \"correlate\": [\n"
                            + "    {\n"
                            + "      \"index\": \"test-index\",\n"
                            + "      \"query\": \"aws.cloudtrail.eventName:ReplicateObject\",\n"
                            + "      \"timestampField\": \"@timestamp\",\n"
                            + "      \"tags\": [\n"
                            + "        \"s3\"\n"
                            + "      ]\n"
                            + "    },\n"
                            + "    {\n"
                            + "      \"index\": \"app_logs\",\n"
                            + "      \"query\": \"keywords:PermissionDenied\",\n"
                            + "      \"timestampField\": \"@timestamp\",\n"
                            + "      \"tags\": [\n"
                            + "        \"others_application\"\n"
                            + "      ]\n"
                            + "    }\n"
                            + "  ]\n"
                            + "}"
                    )
                );
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
        };
        return new TransportIndexCorrelationAction(
            transportService,
            client,
            xContentRegistry,
            Settings.EMPTY,
            actionFilters,
            clusterService
        );
    }

    public void testDoExecute() {
        TransportIndexCorrelationAction action = createAction();

        IndexCorrelationRequest request = new IndexCorrelationRequest("test-index", "test-event", false);
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(IndexCorrelationResponse response) {
                Assert.assertEquals(RestStatus.OK, response.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }
}
