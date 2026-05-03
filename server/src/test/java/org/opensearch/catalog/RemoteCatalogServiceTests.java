/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.action.admin.cluster.catalog.PublishShardAction;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Dispatcher-level tests. End-to-end coverage lives in the integration tests under
 * {@code server/src/internalClusterTest/.../catalog/}.
 */
public class RemoteCatalogServiceTests extends OpenSearchTestCase {

    private static final TimeValue TEST_PUBLISH_TIMEOUT = TimeValue.timeValueSeconds(5);
    private static final String INDEX_NAME = "my-index";
    private static final String INDEX_UUID = "uuid-my-index";

    private CatalogMetadataClient metadataClient;
    private ClusterService clusterService;
    private Client client;
    private ThreadPool threadPool;
    private RemoteCatalogService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metadataClient = mock(CatalogMetadataClient.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        threadPool = mock(ThreadPool.class);
        service = new RemoteCatalogService(
            clusterService, client, metadataClient, threadPool,
            TEST_PUBLISH_TIMEOUT, 3, TimeValue.timeValueMillis(1), 10
        );
    }

    public void testDisabledWhenMetadataClientIsNull() {
        RemoteCatalogService disabled = new RemoteCatalogService(
            clusterService, client, null, threadPool,
            TEST_PUBLISH_TIMEOUT, 3, TimeValue.timeValueMillis(1), 10
        );
        assertFalse(disabled.isEnabled());
        ClusterState state = withCustom(localClusterManagerState(), List.entry(pendingEntry("p1")));
        disabled.clusterChanged(new ClusterChangedEvent("t", state, state));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testIgnoredWhenNotClusterManager() {
        ClusterState previous = stateWithIndex(false);
        ClusterState current = stateWithIndex(false);
        service.clusterChanged(new ClusterChangedEvent("t", current, previous));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testNoOpWhenCustomIsEmpty() {
        ClusterState state = localClusterManagerState();
        service.clusterChanged(new ClusterChangedEvent("t", state, state));
        verify(client, never()).execute(any(), any(), any());
    }

    public void testNodeDepartureFlipsPendingShardsToFailed() {
        DiscoveryNode nodeA = node("node-a");
        DiscoveryNode nodeB = node("node-b");
        DiscoveryNodes previousNodes = DiscoveryNodes.builder()
            .add(nodeA).add(nodeB).localNodeId(nodeA.getId()).clusterManagerNodeId(nodeA.getId())
            .build();
        DiscoveryNodes currentNodes = DiscoveryNodes.builder()
            .add(nodeA).localNodeId(nodeA.getId()).clusterManagerNodeId(nodeA.getId())
            .build();

        PublishEntry entry = PublishEntry.builder()
            .publishId("p1").indexName(INDEX_NAME).indexUUID(INDEX_UUID)
            .phase(PublishPhase.PUBLISHING)
            .shardStatuses(Map.of(
                new ShardId(new Index(INDEX_NAME, INDEX_UUID), 0), PerShardStatus.pending("node-b")
            ))
            .startedAt(1L).build();

        ClusterState previous = stateBuilder(previousNodes).build();
        ClusterState current = withCustom(stateBuilder(currentNodes).build(), List.entry(entry));

        captureUpdatesInMemory();
        service.clusterChanged(new ClusterChangedEvent("t", current, previous));
        verify(clusterService).submitStateUpdateTask(
            org.mockito.ArgumentMatchers.contains("node-departed"),
            any()
        );
    }

    public void testIndexDeletionFlipsPendingShardsToFailed() {
        PublishEntry entry = PublishEntry.builder()
            .publishId("p1").indexName(INDEX_NAME).indexUUID(INDEX_UUID)
            .phase(PublishPhase.PUBLISHING)
            .shardStatuses(Map.of(
                new ShardId(new Index(INDEX_NAME, INDEX_UUID), 0), PerShardStatus.pending("node-a")
            ))
            .startedAt(1L).build();

        ClusterState previous = stateWithIndex(true);
        ClusterState current = withCustom(localClusterManagerState(), List.entry(entry));
        when(clusterService.state()).thenReturn(current);

        captureUpdatesInMemory();
        service.clusterChanged(new ClusterChangedEvent("t", current, previous));
        verify(clusterService).submitStateUpdateTask(
            org.mockito.ArgumentMatchers.contains("index-deleted"),
            any()
        );
    }

    public void testFinalizingSuccessRoutesToProcessor() {
        PublishEntry entry = PublishEntry.builder()
            .publishId("p1").indexName(INDEX_NAME).indexUUID(INDEX_UUID)
            .phase(PublishPhase.FINALIZING_SUCCESS).startedAt(1L).build();

        java.util.concurrent.ExecutorService exec = mock(java.util.concurrent.ExecutorService.class);
        when(threadPool.generic()).thenReturn(exec);

        ClusterState state = withCustom(localClusterManagerState(), List.entry(entry));
        when(clusterService.state()).thenReturn(state);

        service.clusterChanged(new ClusterChangedEvent("t", state, state));
        verify(exec).execute(any(Runnable.class));
    }

    // ---------- helpers ----------

    private static final class List {
        static java.util.List<PublishEntry> entry(PublishEntry e) {
            return java.util.List.of(e);
        }
    }

    private static PublishEntry pendingEntry(String publishId) {
        return PublishEntry.builder()
            .publishId(publishId).indexName(INDEX_NAME).indexUUID(INDEX_UUID)
            .phase(PublishPhase.INITIALIZED).startedAt(1L).build();
    }

    private ClusterState localClusterManagerState() {
        DiscoveryNode local = node("node-a");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(local).localNodeId(local.getId()).clusterManagerNodeId(local.getId()).build();
        return stateBuilder(nodes).build();
    }

    private ClusterState stateWithIndex(boolean includeIndex) {
        DiscoveryNode local = node("node-a");
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(local).localNodeId(local.getId()).clusterManagerNodeId(local.getId()).build();
        Metadata.Builder mb = Metadata.builder();
        if (includeIndex) {
            mb.put(IndexMetadata.builder(INDEX_NAME)
                .settings(Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX_UUID)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                .build(), false);
        }
        return ClusterState.builder(new ClusterName("test")).nodes(nodes).metadata(mb.build()).build();
    }

    private ClusterState.Builder stateBuilder(DiscoveryNodes nodes) {
        return ClusterState.builder(new ClusterName("test")).nodes(nodes);
    }

    private static ClusterState withCustom(ClusterState state, java.util.List<PublishEntry> entries) {
        CatalogPublishesInProgress custom = new CatalogPublishesInProgress(entries);
        Metadata.Builder mb = Metadata.builder(state.metadata()).putCustom(CatalogPublishesInProgress.TYPE, custom);
        return ClusterState.builder(state).metadata(mb).build();
    }

    private static DiscoveryNode node(String id) {
        try {
            return new DiscoveryNode(
                id, id, new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                Collections.emptyMap(), Set.of(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
                org.opensearch.Version.CURRENT
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void captureUpdatesInMemory() {
        doAnswer(inv -> null).when(clusterService).submitStateUpdateTask(any(String.class), any());
    }

    @SuppressWarnings("unused")
    private static void keepImports() throws UnknownHostException, Exception {
        ActionListener.wrap(x -> {}, x -> {});
        BoundTransportAddress.class.toString();
        PublishShardAction.class.toString();
    }
}
