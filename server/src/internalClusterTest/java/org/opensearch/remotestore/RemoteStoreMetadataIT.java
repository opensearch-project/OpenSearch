/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadata;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreMetadataIT extends RemoteStoreBaseIntegTestCase {

    private static final Logger logger = LogManager.getLogger(RemoteStoreMetadataIT.class);
    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class))
            .collect(Collectors.toList());
    }

   @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
    }

    @After
    public void cleanup() throws Exception {
        // Clean up indices after each test
        assertAcked(client().admin().indices().prepareDelete("*").get());
        assertBusy(() -> {
            assertTrue(client().admin().indices().prepareGetIndex().get().getIndices().length == 0);
        }, 30, TimeUnit.SECONDS);
    }

    public void setup() {
        internalCluster().startNodes(3);
    }

    public void testMetadataResponseFromAllNodes() throws Exception {
        setup();

        // Step 1 - We create the cluster, create an index with remote store settings, and index some documents.
        // This sets up the environment and ensures that segment and translog metadata are generated and uploaded.
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        indexDocs();

        ClusterState state = getClusterState();
        List<String> nodes = state.nodes().getNodes().values().stream()
            .map(DiscoveryNode::getName)
            .collect(Collectors.toList());

        // Step 2 - We collect all node names in the cluster and send RemoteStoreMetadata API requests from each node
        // to verify that segment and translog metadata can be fetched successfully from all data nodes.
        String shardId = "0";
        for (String node : nodes) {
            assertBusy(() -> {
                RemoteStoreMetadataResponse response = client(node).admin()
                    .cluster()
                    .prepareRemoteStoreMetadata(INDEX_NAME, shardId)
                    .setTimeout(TimeValue.timeValueSeconds(30))
                    .get();

                assertTrue(response.getSuccessfulShards() > 0);
                Map<String, Map<Integer, List<RemoteStoreMetadata>>> indexMetadata = response.groupByIndexAndShards();
                assertNotNull("Metadata should not be null", indexMetadata);
                assertTrue("Index metadata should exist", indexMetadata.containsKey(INDEX_NAME));
                assertTrue("Shard metadata should exist", indexMetadata.get(INDEX_NAME).containsKey(0));

                List<RemoteStoreMetadata> shardMetadata = indexMetadata.get(INDEX_NAME).get(0);
                assertFalse("Shard metadata should not be empty", shardMetadata.isEmpty());
                validateSegmentMetadata(shardMetadata.get(0));
                validateTranslogMetadata(shardMetadata.get(0));
            }, 30, TimeUnit.SECONDS);
        }

        changeReplicaCountAndEnsureGreen(1);
        verifyDocumentCount();

        for (String node : nodes) {
            assertBusy(() -> {
                RemoteStoreMetadataResponse response = client(node).admin()
                    .cluster()
                    .prepareRemoteStoreMetadata(INDEX_NAME, shardId)
                    .get();

                Map<String, Map<Integer, List<RemoteStoreMetadata>>> indexMetadata = response.groupByIndexAndShards();
                List<RemoteStoreMetadata> shardMetadata = indexMetadata.get(INDEX_NAME).get(0);

                assertFalse("Shard metadata should not be empty", shardMetadata.isEmpty());
                validateSegmentMetadata(shardMetadata.get(0));
                validateTranslogMetadata(shardMetadata.get(0));
            }, 30, TimeUnit.SECONDS);
        }
    }


    public void testMetadataResponseAllShards() throws Exception {
        setup();

        // Step 1 - We created multi-shard index and index some documents
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0, 3));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        indexDocs();

        // Step 3 - We invoke the RemoteStoreMetadata API without specifying a shard,
        // which returns metadata for all shards of the given index. We then validate the response for each shard.
        assertBusy(() -> {
            RemoteStoreMetadataResponse response = client().admin()
                .cluster()
                .prepareRemoteStoreMetadata(INDEX_NAME, null)
                .get();

            assertEquals(3, response.getSuccessfulShards());
            Map<String, Map<Integer, List<RemoteStoreMetadata>>> indexMetadata = response.groupByIndexAndShards();
            assertNotNull("Metadata should not be null", indexMetadata);
            assertEquals(1, indexMetadata.size());
            assertEquals(3, indexMetadata.get(INDEX_NAME).size());

            for (int shardId = 0; shardId < 3; shardId++) {
                List<RemoteStoreMetadata> shardMetadata = indexMetadata.get(INDEX_NAME).get(shardId);
                assertNotNull("Metadata for shard " + shardId + " should not be null", shardMetadata);
                assertFalse("Metadata for shard " + shardId + " should not be empty", shardMetadata.isEmpty());
                validateSegmentMetadata(shardMetadata.get(0));
                validateTranslogMetadata(shardMetadata.get(0));
            }
        }, 30, TimeUnit.SECONDS);
    }

    private void indexDocs() {
        try {
            // Created sample documents with actual content
            List<Map<String, Object>> documents = new ArrayList<>();
            
            int numDocs = randomIntBetween(5, 10);
            for (int i = 0; i < numDocs; i++) {
                Map<String, Object> doc = new HashMap<>();
                doc.put("title", "Test Document " + (i + 1));
                doc.put("content", "This is test document number " + (i + 1) + " with some content.");
                doc.put("timestamp", System.currentTimeMillis());
                doc.put("doc_id", i);
                documents.add(doc);

                IndexResponse response = client().prepareIndex(INDEX_NAME)
                    .setSource(doc, XContentType.JSON)
                    .get();
                
                assertEquals(RestStatus.CREATED, response.status());

                if (randomBoolean()) {
                    flush(INDEX_NAME);
                } else {
                    refresh(INDEX_NAME);
                }
            }

            // Ensure documents are indexed
            refresh(INDEX_NAME);
            verifyDocumentCount();
        } catch (Exception e) {
            logger.error("Failed to index documents", e);
            fail("Failed to index documents: " + e.getMessage());
        }
    }

    private void verifyDocumentCount() throws Exception {
        assertBusy(() -> {
            SearchResponse searchResponse = client().prepareSearch(INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .get();
            
            assertTrue("Documents should be indexed", searchResponse.getHits().getTotalHits().value() > 0);
        }, 30, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private void validateSegmentMetadata(RemoteStoreMetadata metadata) {
        Map<String, Object> metadataMap = toMap(metadata);
        Map<String, Object> segments = (Map<String, Object>) metadataMap.get("segments");
        
        assertNotNull("Segments metadata should not be null", segments);
        
        if (!segments.isEmpty()) {
            segments.values().forEach(value -> {
                if (value instanceof Map) {
                    Map<String, Object> segmentInfo = (Map<String, Object>) value;
                    assertNotNull("Generation should not be null", segmentInfo.get("generation"));
                    assertNotNull("Primary term should not be null", segmentInfo.get("primary_term"));
                    
                    Object uploadedSegmentsObj = segmentInfo.get("uploaded_segments");
                    if (uploadedSegmentsObj instanceof Map) {
                        Map<String, Object> uploadedSegments = (Map<String, Object>) uploadedSegmentsObj;
                        if (!uploadedSegments.isEmpty()) {
                            uploadedSegments.values().forEach(segmentFile -> {
                                if (segmentFile instanceof Map) {
                                    Map<String, Object> fileInfo = (Map<String, Object>) segmentFile;
                                    assertNotNull("Original name should not be null", fileInfo.get("original_name"));
                                    assertNotNull("Checksum should not be null", fileInfo.get("checksum"));
                                    assertNotNull("Length should not be null", fileInfo.get("length"));
                                }
                            });
                        }
                    }
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    private void validateTranslogMetadata(RemoteStoreMetadata metadata) {
        Map<String, Object> metadataMap = toMap(metadata);
        Map<String, Object> translog = (Map<String, Object>) metadataMap.get("translog");
        
        assertNotNull("Translog metadata should not be null", translog);
        
        if (!translog.isEmpty()) {
            translog.values().forEach(value -> {
                if (value instanceof Map) {
                    Map<String, Object> translogInfo = (Map<String, Object>) value;
                    assertNotNull("Generation should not be null", translogInfo.get("generation"));
                    assertNotNull("Primary term should not be null", translogInfo.get("primary_term"));
                    assertNotNull("Min translog generation should not be null", translogInfo.get("min_translog_gen"));
                    
                    Object contentObj = translogInfo.get("content");
                    if (contentObj instanceof Map) {
                        Map<String, Object> content = (Map<String, Object>) contentObj;
                        assertNotNull("Content should not be null", content);
                        assertNotNull("Content generation should not be null", content.get("generation"));
                        assertNotNull("Content primary term should not be null", content.get("primary_term"));
                        assertNotNull("Content min translog generation should not be null", 
                            content.get("min_translog_generation"));
                    }
                }
            });
        }
    }

    private Map<String, Object> toMap(RemoteStoreMetadata metadata) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            metadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference bytes = BytesReference.bytes(builder);
            
            try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, bytes.streamInput())) {
                return parser.map();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert metadata to map", e);
        }
    }

    private void changeReplicaCountAndEnsureGreen(int replicaCount) {
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(SETTING_NUMBER_OF_REPLICAS, replicaCount))
        );
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }
}