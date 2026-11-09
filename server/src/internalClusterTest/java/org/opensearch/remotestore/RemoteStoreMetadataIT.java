/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreShardMetadata;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService.TestPlugin;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasKey;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMetadataIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-meta-api-test";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(TestPlugin.class)).collect(Collectors.toList());
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

                    for (RemoteStoreShardMetadata metadata : metadataList) {
                        assertEquals(index, metadata.getIndexName());
                        assertEquals((int) shardId, metadata.getShardId());

                        assertNotNull(metadata.getLatestSegmentMetadataFileName());
                        assertNotNull(metadata.getLatestTranslogMetadataFileName());

                        Map<String, Map<String, Object>> segmentFiles = metadata.getSegmentMetadataFiles();
                        assertNotNull(segmentFiles);
                        assertFalse(segmentFiles.isEmpty());

                        for (Map<String, Object> fileMeta : segmentFiles.values()) {
                            Map<String, Object> files = (Map<String, Object>) fileMeta.get("files");
                            assertNotNull(files);
                            assertFalse(files.isEmpty());
                            for (Object value : files.values()) {
                                Map<String, Object> meta = (Map<String, Object>) value;
                                assertThat(meta, allOf(hasKey("original_name"), hasKey("checksum"), hasKey("length")));
                            }

                            Map<String, Object> checkpoint = (Map<String, Object>) fileMeta.get("replication_checkpoint");
                            assertNotNull(checkpoint);
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
                        }

                        Map<String, Map<String, Object>> translogFiles = metadata.getTranslogMetadataFiles();
                        assertNotNull(translogFiles);
                        assertFalse(translogFiles.isEmpty());
                        for (Map<String, Object> translogMeta : translogFiles.values()) {
                            assertThat(
                                translogMeta,
                                allOf(
                                    hasKey("primary_term"),
                                    hasKey("generation"),
                                    hasKey("min_translog_gen"),
                                    hasKey("generation_to_primary_term")
                                )
                            );
                        }
                    }
                });
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testMetadataResponseAllShards() throws Exception {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 2));
        ensureGreen(INDEX_NAME);
        indexDocs();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertBusy(() -> { assertFalse(client().admin().cluster().prepareHealth(INDEX_NAME).get().isTimedOut()); });

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();

        response.groupByIndexAndShards().forEach((index, shardMap) -> {
            shardMap.forEach((shardId, metadataList) -> {
                assertFalse(metadataList.isEmpty());

                for (RemoteStoreShardMetadata metadata : metadataList) {
                    assertEquals(index, metadata.getIndexName());
                    assertEquals((int) shardId, metadata.getShardId());

                    assertNotNull(metadata.getLatestSegmentMetadataFileName());
                    assertNotNull(metadata.getLatestTranslogMetadataFileName());

                    Map<String, Map<String, Object>> segmentFiles = metadata.getSegmentMetadataFiles();
                    assertNotNull(segmentFiles);
                    assertFalse(segmentFiles.isEmpty());

                    for (Map<String, Object> fileMeta : segmentFiles.values()) {
                        Map<String, Object> files = (Map<String, Object>) fileMeta.get("files");
                        assertNotNull(files);
                        assertFalse(files.isEmpty());
                        for (Object value : files.values()) {
                            Map<String, Object> meta = (Map<String, Object>) value;
                            assertThat(meta, allOf(hasKey("original_name"), hasKey("checksum"), hasKey("length")));
                        }

                        Map<String, Object> checkpoint = (Map<String, Object>) fileMeta.get("replication_checkpoint");
                        assertNotNull(checkpoint);
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
                    }

                    Map<String, Map<String, Object>> translogFiles = metadata.getTranslogMetadataFiles();
                    assertNotNull(translogFiles);
                    assertFalse(translogFiles.isEmpty());
                    for (Map<String, Object> translogMeta : translogFiles.values()) {
                        assertThat(
                            translogMeta,
                            allOf(
                                hasKey("primary_term"),
                                hasKey("generation"),
                                hasKey("min_translog_gen"),
                                hasKey("generation_to_primary_term")
                            )
                        );
                    }
                }
            });
        });
    }

    public void testMultipleMetadataFilesPerShard() throws Exception {
        setup();

        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 1));
        ensureGreen(INDEX_NAME);

        int refreshCount = 5;
        for (int i = 0; i < refreshCount; i++) {
            indexDocs();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();

        response.groupByIndexAndShards().forEach((index, shardMap) -> {
            shardMap.forEach((shardId, metadataList) -> {
                assertFalse(metadataList.isEmpty());

                for (RemoteStoreShardMetadata metadata : metadataList) {
                    assertEquals(refreshCount, metadata.getSegmentMetadataFiles().size());
                    assertTrue(metadata.getTranslogMetadataFiles().size() >= 1);
                }
            });
        });
    }

    public void testMetadataResponseMultipleIndicesAndShards() throws Exception {
        setup();

        String index1 = INDEX_NAME + "-1";
        String index2 = INDEX_NAME + "-2";

        createIndex(index1, remoteStoreIndexSettings(0, 2));
        createIndex(index2, remoteStoreIndexSettings(0, 3));
        ensureGreen(index1, index2);

        indexDocs(index1);
        indexDocs(index2);

        client().admin().indices().prepareRefresh(index1).get();
        client().admin().indices().prepareRefresh(index2).get();

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata("*", null).get();

        Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> grouped = response.groupByIndexAndShards();

        assertTrue(grouped.containsKey(index1));
        assertTrue(grouped.containsKey(index2));

        grouped.forEach((index, shardMap) -> {
            shardMap.forEach((shardId, metadataList) -> {
                assertFalse(metadataList.isEmpty());
                metadataList.forEach(metadata -> {
                    assertEquals(index, metadata.getIndexName());
                    assertEquals((int) shardId, metadata.getShardId());
                    assertNotNull(metadata.getSegmentMetadataFiles());
                    assertFalse(metadata.getSegmentMetadataFiles().isEmpty());
                    assertNotNull(metadata.getTranslogMetadataFiles());
                    assertFalse(metadata.getTranslogMetadataFiles().isEmpty());
                });
            });
        });
    }

    public void testShardFailureWhenRepositoryMissing() throws Exception {
        String indexName = "failure-case-index";

        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.replication.type", "SEGMENT")
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put("index.remote_store.repository", "non-existent-repo")
            .put("index.remote_store.translog.enabled", true)
            .put("index.remote_store.translog.repository", "non-existent-repo");

        Exception exception = expectThrows(Exception.class, () -> assertAcked(prepareCreate(indexName).setSettings(settings)));

        assertTrue(exception.getMessage().toLowerCase(Locale.ROOT).contains("repository"));
    }

    private void indexDocs() {
        indexDocs(INDEX_NAME);
    }

    private void indexDocs(String indexName) {
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            client().prepareIndex(indexName).setId("doc-" + i).setSource("field", "value-" + i).get();
        }
    }
}
