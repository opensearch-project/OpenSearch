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
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;

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

    @SuppressWarnings("unchecked")
    public void testMetadataResponseFromAllNodes() {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureGreen(INDEX_NAME);
        indexDocs();
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

                        Map<String, Object> segments = metadata.getSegments();
                        assertNotNull(segments);
                        assertTrue(segments.containsKey("files"));
                        Map<String, Object> files = (Map<String, Object>) segments.get("files");
                        assertFalse(files.isEmpty());

                        for (Map.Entry<String, Object> entry : files.entrySet()) {
                            Map<String, Object> fileMeta = (Map<String, Object>) entry.getValue();
                            assertThat(fileMeta, allOf(hasKey("original_name"), hasKey("checksum"), hasKey("length")));
                        }

                        assertTrue(segments.containsKey("replication_checkpoint"));
                        Map<String, Object> checkpoint = (Map<String, Object>) segments.get("replication_checkpoint");
                        assertThat(
                            checkpoint,
                            allOf(
                                hasKey("primary_term"),
                                hasKey("segments_gen"),
                                hasKey("segment_infos_version"),
                                hasKey("codec"),
                                hasKey("created_timestamp")
                            )
                        );

                        Map<String, Object> translog = metadata.getTranslog();
                        assertNotNull(translog);
                        assertThat(
                            translog,
                            allOf(
                                hasKey("primary_term"),
                                hasKey("generation"),
                                hasKey("min_translog_gen"),
                                hasKey("generation_to_primary_term")
                            )
                        );
                    }
                });
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testMetadataResponseAllShards() {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 2));
        ensureGreen(INDEX_NAME);
        indexDocs();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();
        assertEquals(2, response.getSuccessfulShards());

        response.groupByIndexAndShards().forEach((index, shardMap) -> {
            shardMap.forEach((shardId, metadataList) -> {
                assertFalse(metadataList.isEmpty());

                for (RemoteStoreMetadata metadata : metadataList) {
                    assertEquals(index, metadata.getIndexName());
                    assertEquals((int) shardId, metadata.getShardId());

                    Map<String, Object> segments = metadata.getSegments();
                    assertNotNull(segments);
                    assertTrue(segments.containsKey("files"));
                    Map<String, Object> files = (Map<String, Object>) segments.get("files");
                    assertFalse(files.isEmpty());

                    for (Map.Entry<String, Object> entry : files.entrySet()) {
                        Map<String, Object> fileMeta = (Map<String, Object>) entry.getValue();
                        assertThat(fileMeta, allOf(hasKey("original_name"), hasKey("checksum"), hasKey("length")));
                    }

                    assertTrue(segments.containsKey("replication_checkpoint"));
                    Map<String, Object> checkpoint = (Map<String, Object>) segments.get("replication_checkpoint");
                    assertThat(
                        checkpoint,
                        allOf(
                            hasKey("primary_term"),
                            hasKey("segments_gen"),
                            hasKey("segment_infos_version"),
                            hasKey("codec"),
                            hasKey("created_timestamp")
                        )
                    );

                    Map<String, Object> translog = metadata.getTranslog();
                    assertNotNull(translog);
                    assertThat(
                        translog,
                        allOf(
                            hasKey("primary_term"),
                            hasKey("generation"),
                            hasKey("min_translog_gen"),
                            hasKey("generation_to_primary_term")
                        )
                    );
                }
            });
        });
    }

    private void indexDocs() {
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            client().prepareIndex(INDEX_NAME).setId("doc-" + i).setSource("field", "value-" + i).get();
        }
    }
}
