/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.MetadataIndexTemplateService.PutRequest;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_REPLICATION_TYPE_SETTING;
import static org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider.INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING;
import static org.opensearch.common.settings.Settings.builder;
import static org.opensearch.env.Environment.PATH_HOME_SETTING;
import static org.opensearch.indices.ShardLimitValidatorTests.createTestShardLimitService;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that index-template validation of {@code index.routing.allocation.total_primary_shards_per_node}
 * uses the cluster/node-aware precondition (is this a remote-store cluster?) rather than the index-local
 * {@code index.remote_store.enabled} flag a template can never carry.
 *
 * <p>Kept in its own class (rather than {@link MetadataIndexTemplateServiceTests}) because these cases are
 * fully mock-driven and do not need the shared single-node fixture; isolating them also avoids perturbing
 * the randomized method ordering of that (state-sharing) suite.
 */
public class MetadataIndexTemplateServiceRemoteStoreTests extends OpenSearchTestCase {

    public void testTotalPrimaryShardsTemplateValidatesOnRemoteStoreCluster() {
        // Every node is a remote-store node → the template must validate successfully.
        PutRequest request = new PutRequest("test", "test_index_primary_shard_constraint_remote");
        request.patterns(singletonList("test_shards_wait*"));
        request.settings(
            builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
                .build()
        );

        DiscoveryNodes remoteNodes = DiscoveryNodes.builder()
            .add(IndexShardTestUtils.getFakeRemoteEnabledNode("node1"))
            .add(IndexShardTestUtils.getFakeRemoteEnabledNode("node2"))
            .build();

        List<Throwable> throwables = putTemplate(xContentRegistry(), request, remoteNodes);
        assertThat(throwables, empty());
    }

    public void testTotalPrimaryShardsTemplateRejectedOnEmptyNodeCluster() {
        // No nodes → not a remote-store cluster (allMatch is vacuously true on an empty set, so the
        // !nodes.isEmpty() guard must reject). Covers the empty-node branch of the hardened check.
        PutRequest request = new PutRequest("test", "test_index_primary_shard_constraint_empty");
        request.patterns(singletonList("test_shards_wait*"));
        request.settings(
            builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
                .build()
        );

        List<Throwable> throwables = putTemplate(xContentRegistry(), request, DiscoveryNodes.EMPTY_NODES);
        assertThat(throwables, not(empty()));
        assertThat(throwables.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(throwables.get(0).getMessage(), containsString("can only be used with remote store enabled clusters"));
    }

    public void testTotalPrimaryShardsTemplateRejectedWhenSomeNodeIsNotRemoteStore() {
        // Mixed cluster (one non-remote node) → not every node is a remote-store node, so the
        // setting must be rejected. Covers the allMatch==false branch of the hardened check.
        PutRequest request = new PutRequest("test", "test_index_primary_shard_constraint_mixed");
        request.patterns(singletonList("test_shards_wait*"));
        request.settings(
            builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "1")
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1")
                .put(INDEX_TOTAL_PRIMARY_SHARDS_PER_NODE_SETTING.getKey(), 2)
                .put(INDEX_REPLICATION_TYPE_SETTING.getKey(), ReplicationType.SEGMENT.toString())
                .build()
        );

        DiscoveryNodes mixedNodes = DiscoveryNodes.builder()
            .add(IndexShardTestUtils.getFakeRemoteEnabledNode("remote1"))
            .add(IndexShardTestUtils.getFakeDiscoNode("plain1"))
            .build();

        List<Throwable> throwables = putTemplate(xContentRegistry(), request, mixedNodes);
        assertThat(throwables, not(empty()));
        assertThat(throwables.get(0), instanceOf(IllegalArgumentException.class));
        assertThat(throwables.get(0).getMessage(), containsString("can only be used with remote store enabled clusters"));
    }

    private static List<Throwable> putTemplate(NamedXContentRegistry xContentRegistry, PutRequest request, DiscoveryNodes discoveryNodes) {
        ClusterService clusterService = mock(ClusterService.class);
        Settings settings = Settings.builder().put(PATH_HOME_SETTING.getKey(), "dummy").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Metadata metadata = Metadata.builder().build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .nodes(discoveryNodes)
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.getSettings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        IndicesService indicesServices = mock(IndicesService.class);
        MetadataCreateIndexService createIndexService = new MetadataCreateIndexService(
            Settings.EMPTY,
            clusterService,
            indicesServices,
            null,
            null,
            createTestShardLimitService(randomIntBetween(1, 1000), false),
            new Environment(builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(), null),
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            null,
            xContentRegistry,
            new SystemIndices(Collections.emptyMap()),
            true,
            new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
            DefaultRemoteStoreSettings.INSTANCE,
            null
        );
        MetadataIndexTemplateService service = new MetadataIndexTemplateService(
            clusterService,
            createIndexService,
            new AliasValidator(),
            null,
            new IndexScopedSettings(Settings.EMPTY, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS),
            xContentRegistry,
            null
        );

        final List<Throwable> throwables = new ArrayList<>();
        service.putTemplate(request, new MetadataIndexTemplateService.PutListener() {
            @Override
            public void onResponse(MetadataIndexTemplateService.PutResponse response) {}

            @Override
            public void onFailure(Exception e) {
                throwables.add(e);
            }
        });
        return throwables;
    }
}
