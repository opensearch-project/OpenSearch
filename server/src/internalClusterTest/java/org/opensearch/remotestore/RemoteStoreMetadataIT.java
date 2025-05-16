/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadata;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMetadataIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-meta-api-test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    public void setup() {
        internalCluster().startNodes(3);
    }

    public void testMetadataResponseFromAllNodes() {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureGreen(INDEX_NAME);
        indexDocs();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream().map(DiscoveryNode::getName).collect(Collectors.toList());

        for (String node : nodes) {
            RemoteStoreMetadataResponse response = client(node).admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();
            assertTrue(response.getSuccessfulShards() > 0);
            assertNotNull(response.groupByIndexAndShards());

            response.groupByIndexAndShards().forEach((index, shardMap) -> {
                shardMap.forEach((shardId, metadataList) -> {
                    assertFalse(metadataList.isEmpty());
                    for (RemoteStoreMetadata metadata : metadataList) {
                        assertEquals(index, metadata.getIndexName());
                        assertEquals((int) shardId, metadata.getShardId());
                        assertNotNull(metadata.getSegments());
                        assertNotNull(metadata.getTranslog());
                        assertFalse(metadata.getSegments().isEmpty());
                        assertFalse(metadata.getTranslog().isEmpty());
                    }
                });
            });
        }
    }

    public void testMetadataResponseAllShards() {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 2));
        ensureGreen(INDEX_NAME);
        indexDocs();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();
        assertEquals(2, response.getSuccessfulShards());

        response.groupByIndexAndShards().forEach((index, shardMap) -> {
            shardMap.forEach((shardId, metadataList) -> {
                assertFalse(metadataList.isEmpty());
                for (RemoteStoreMetadata metadata : metadataList) {
                    assertEquals(index, metadata.getIndexName());
                    assertEquals((int) shardId, metadata.getShardId());
                    assertNotNull(metadata.getSegments());
                    assertNotNull(metadata.getTranslog());
                }
            });
        });
    }

    private void indexDocs() {
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            client().prepareIndex(INDEX_NAME)
                .setId("doc-" + i)
                .setSource("field", "value-" + i)
                .get();
        }
    }
}
