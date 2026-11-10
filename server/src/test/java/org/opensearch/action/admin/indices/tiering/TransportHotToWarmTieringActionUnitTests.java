/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ActionTestUtils;
import org.opensearch.cluster.ClusterInfo;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiskUsage;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.IndexModule;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.opensearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportHotToWarmTieringActionUnitTests extends OpenSearchTestCase {

    private static final String REMOTE_INDEX = "remote-index";
    private static final String REGULAR_INDEX = "regular-index";
    private static final String WARM_NODE_ID = "warm-node";
    private static final String CLUSTER_MANAGER_NODE_ID = "cluster-manager-node";

    private ThreadPool threadPool;
    private ClusterService clusterService;
    private TransportService transportService;
    private ClusterInfoService clusterInfoService;
    private TransportHotToWarmTieringAction transportHotToWarmTieringAction;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);

        MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        clusterInfoService = mock(ClusterInfoService.class);
        transportHotToWarmTieringAction = new TransportHotToWarmTieringAction(
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            clusterInfoService,
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
                .build()
        );
    }

    @Override
    public void tearDown() throws Exception {
        clusterService.close();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        super.tearDown();
    }

    public void testRespondsWhenOnlySomeIndicesAreAccepted() {
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(buildClusterState()));
        when(clusterInfoService.getClusterInfo()).thenReturn(buildClusterInfo());

        TieringIndexRequest request = new TieringIndexRequest(TieringIndexRequest.Tier.WARM.name(), REMOTE_INDEX, REGULAR_INDEX);

        HotToWarmTieringResponse response = ActionTestUtils.executeBlocking(transportHotToWarmTieringAction, request);

        assertTrue(response.isAcknowledged());
        assertEquals(1, response.getFailedIndices().size());
        assertEquals(REGULAR_INDEX, response.getFailedIndices().get(0).getIndex());
        assertEquals("index is not backed up by the remote store", response.getFailedIndices().get(0).getFailureReason());
    }

    private ClusterState buildClusterState() {
        String remoteIndexUuid = randomAlphaOfLength(10);
        String regularIndexUuid = randomAlphaOfLength(10);

        IndexMetadata remoteIndexMetadata = IndexMetadata.builder(REMOTE_INDEX)
            .settings(
                Settings.builder()
                    .put(createDefaultIndexSettings(remoteIndexUuid))
                    .put(IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING.getKey(), true)
                    .put(IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT)
                    .put(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT)
            )
            .build();
        IndexMetadata regularIndexMetadata = IndexMetadata.builder(REGULAR_INDEX)
            .settings(
                Settings.builder()
                    .put(createDefaultIndexSettings(regularIndexUuid))
                    .put(IndexModule.INDEX_TIERING_STATE.getKey(), IndexModule.TieringState.HOT)
            )
            .build();

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder(remoteIndexMetadata))
            .put(IndexMetadata.builder(regularIndexMetadata))
            .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(remoteIndexMetadata).addAsNew(regularIndexMetadata).build();

        DiscoveryNode clusterManagerNode = new DiscoveryNode(
            CLUSTER_MANAGER_NODE_ID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        DiscoveryNode warmNode = new DiscoveryNode(
            WARM_NODE_ID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.WARM_ROLE),
            Version.CURRENT
        );
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(clusterManagerNode)
            .add(warmNode)
            .localNodeId(CLUSTER_MANAGER_NODE_ID)
            .clusterManagerNodeId(CLUSTER_MANAGER_NODE_ID)
            .build();

        return ClusterState.builder(new ClusterName("test-cluster")).metadata(metadata).routingTable(routingTable).nodes(nodes).build();
    }

    private ClusterInfo buildClusterInfo() {
        return new ClusterInfo(
            Map.of(WARM_NODE_ID, new DiskUsage(WARM_NODE_ID, WARM_NODE_ID, "/warm", 100L, 50L)),
            null,
            Map.of("[" + REMOTE_INDEX + "][0][p]", 10L),
            null,
            null,
            Map.of(),
            Map.of()
        );
    }

    private Settings createDefaultIndexSettings(String indexUuid) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .put("index.version.created", Version.CURRENT)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
    }
}
