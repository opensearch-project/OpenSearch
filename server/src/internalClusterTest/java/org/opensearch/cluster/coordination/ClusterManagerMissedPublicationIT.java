/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.opensearch.cluster.IncompatibleClusterStateVersionException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.DISCOVERY;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A data node stays connected to the cluster manager but misses one cluster state publication.
 * It can't apply the next diff, so the cluster manager sends it the full state.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterManagerMissedPublicationIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test_index_name";
    private static final String FIRST_TEMPLATE_NAME = "first_template";
    private static final String SECOND_TEMPLATE_NAME = "second_template";
    private static final String THIRD_TEMPLATE_NAME = "third_template";
    private static final long AWAIT_SECONDS = 30;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testDataNodeGetsFullStateAfterMissingPublication() throws Exception {
        final Settings nodeSettings = Settings.builder().put(Coordinator.PUBLISH_TIMEOUT_SETTING.getKey(), "5s").build();
        final String clusterManager = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        final String dataNode = internalCluster().startDataOnlyNode(nodeSettings);
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            )
        );
        ensureGreen(INDEX_NAME);

        final ClusterService dataNodeClusterService = internalCluster().clusterService(dataNode);
        final DiscoveryNode clusterManagerBefore = dataNodeClusterService.state().nodes().getClusterManagerNode();
        final PublishClusterStateStats statsBefore = publishStats(dataNode);

        final CountDownLatch firstApplied = latchOnTemplateApplied(dataNodeClusterService, FIRST_TEMPLATE_NAME);
        final CountDownLatch thirdApplied = latchOnTemplateApplied(dataNodeClusterService, THIRD_TEMPLATE_NAME);

        final AtomicReference<IncompatibleClusterStateVersionException> incompatibleDiff = new AtomicReference<>();
        ((MockTransportService) internalCluster().getInstance(TransportService.class, dataNode)).addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                try {
                    handler.messageReceived(request, channel, task);
                } catch (IncompatibleClusterStateVersionException e) {
                    incompatibleDiff.set(e);
                    throw e;
                }
            }
        );

        final AtomicBoolean dropNextPublication = new AtomicBoolean();
        final CountDownLatch publicationDropped = new CountDownLatch(1);
        ((MockTransportService) internalCluster().getInstance(TransportService.class, clusterManager)).addSendBehavior(
            internalCluster().getInstance(TransportService.class, dataNode),
            (connection, requestId, action, request, options) -> {
                if (PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME.equals(action)
                    && dropNextPublication.compareAndSet(true, false)) {
                    publicationDropped.countDown();
                    throw new ConnectTransportException(connection.getNode(), "Simulated missed publication", action);
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        assertAcked(putTemplate(FIRST_TEMPLATE_NAME).get());
        assertTrue(firstApplied.await(AWAIT_SECONDS, TimeUnit.SECONDS));

        dropNextPublication.set(true);
        putTemplate(SECOND_TEMPLATE_NAME).execute();
        assertTrue(publicationDropped.await(AWAIT_SECONDS, TimeUnit.SECONDS));
        assertFalse(dataNodeClusterService.state().metadata().templates().containsKey(SECOND_TEMPLATE_NAME));

        putTemplate(THIRD_TEMPLATE_NAME).execute();
        assertTrue("The data node didn't apply the full cluster state", thirdApplied.await(AWAIT_SECONDS, TimeUnit.SECONDS));

        assertNotNull("The data node should have rejected the diff", incompatibleDiff.get());
        final PublishClusterStateStats statsAfter = publishStats(dataNode);
        assertEquals(
            statsBefore.getIncompatibleClusterStateDiffReceivedCount() + 1,
            statsAfter.getIncompatibleClusterStateDiffReceivedCount()
        );
        assertEquals(statsBefore.getFullClusterStateReceivedCount() + 1, statsAfter.getFullClusterStateReceivedCount());
        assertEquals(clusterManagerBefore, dataNodeClusterService.state().nodes().getClusterManagerNode());
        assertTrue(
            internalCluster().getInstance(IndicesService.class, dataNode)
                .hasIndex(dataNodeClusterService.state().metadata().index(INDEX_NAME).getIndex())
        );
    }

    private static PutIndexTemplateRequestBuilder putTemplate(String name) {
        return client().admin().indices().preparePutTemplate(name).setPatterns(List.of(name + "-*"));
    }

    private static PublishClusterStateStats publishStats(String node) {
        return client().admin()
            .cluster()
            .prepareNodesStats(node)
            .clear()
            .addMetric(DISCOVERY.metricName())
            .get()
            .getNodes()
            .getFirst()
            .getDiscoveryStats()
            .getPublishStats();
    }

    private static CountDownLatch latchOnTemplateApplied(ClusterService clusterService, String template) {
        final CountDownLatch latch = new CountDownLatch(1);
        clusterService.addListener(event -> {
            if (event.state().metadata().templates().containsKey(template)) {
                latch.countDown();
            }
        });
        return latch;
    }
}
