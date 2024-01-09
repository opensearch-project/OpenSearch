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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsRequest;
import org.opensearch.plugin.correlation.events.action.SearchCorrelatedEventsResponse;
import org.opensearch.plugin.correlation.events.model.EventWithScore;
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
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;

public class TransportSearchCorrelatedEventsActionTests extends OpenSearchTestCase {

    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportSearchCorrelatedEventsActionTests");
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
        clusterService = createClusterService(threadPool, discoveryNode);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    private TransportSearchCorrelatedEventsAction createAction() {
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
                            + "  \"index2\": \"correlated-index\"\n"
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
        };

        return new TransportSearchCorrelatedEventsAction(transportService, client, actionFilters);
    }

    public void testDoExecute() {
        TransportSearchCorrelatedEventsAction action = createAction();

        SearchCorrelatedEventsRequest request = new SearchCorrelatedEventsRequest("test-index", "test-event", "@timestamp", 300000L, 5);
        action.doExecute(null, request, new ActionListener<>() {
            @Override
            public void onResponse(SearchCorrelatedEventsResponse response) {
                List<EventWithScore> events = response.getEvents();
                for (EventWithScore event : events) {
                    Assert.assertEquals("correlated-index", event.getIndex());
                    Assert.assertEquals("correlated-event", event.getEvent());
                    Assert.assertEquals(1.0, event.getScore(), 0.0);
                    Assert.assertTrue(event.getTags().isEmpty());
                }

                Assert.assertEquals(RestStatus.OK, response.getStatus());
            }

            @Override
            public void onFailure(Exception e) {
                // ignore
            }
        });
    }
}
