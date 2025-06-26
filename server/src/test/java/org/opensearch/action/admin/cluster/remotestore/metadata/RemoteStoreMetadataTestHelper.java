/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for unit testing RemoteStoreMetadata and RemoteStoreMetadataResponse.
 */
public class RemoteStoreMetadataTestHelper {

    @SuppressWarnings("unchecked")
    public static RemoteStoreShardMetadata createTestMetadata(
        Map<String, Object> segmentMetadata,
        Map<String, Object> translogMetadata,
        String indexName,
        int shardId
    ) {
        Map<String, Map<String, Object>> segmentFiles = (Map<String, Map<String, Object>>) (Map<?, ?>) segmentMetadata;
        Map<String, Map<String, Object>> translogFiles = (Map<String, Map<String, Object>>) (Map<?, ?>) translogMetadata;

        String latestSegmentFile = segmentFiles.isEmpty() ? null : segmentFiles.keySet().iterator().next();
        String latestTranslogFile = translogFiles.isEmpty() ? null : translogFiles.keySet().iterator().next();

        return new RemoteStoreShardMetadata(indexName, shardId, segmentFiles, translogFiles, latestSegmentFile, latestTranslogFile);
    }

    public static Map<String, Object> createTestSegmentMetadata() {
        Map<String, Object> uploadedSegment = new HashMap<>();
        uploadedSegment.put("original_name", "segment_1");
        uploadedSegment.put("checksum", "abc123");
        uploadedSegment.put("length", 1024L);

        Map<String, Object> uploadedSegments = new HashMap<>();
        uploadedSegments.put("seg_1", uploadedSegment);

        Map<String, Object> replicationCheckpoint = new HashMap<>();
        replicationCheckpoint.put("shard_id", "index[0]");
        replicationCheckpoint.put("primary_term", 1L);
        replicationCheckpoint.put("generation", 1L);
        replicationCheckpoint.put("version", 1L);
        replicationCheckpoint.put("length", 12345L);
        replicationCheckpoint.put("codec", "Lucene80");
        replicationCheckpoint.put("created_timestamp", System.currentTimeMillis());

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("files", uploadedSegments);
        metadata.put("replication_checkpoint", replicationCheckpoint);

        return Map.of("metadata__segment1", metadata);
    }

    public static Map<String, Object> createTestTranslogMetadata() {
        Map<String, String> genToTermMap = new HashMap<>();
        genToTermMap.put("1", "1");

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("primary_term", 1L);
        metadata.put("generation", 1L);
        metadata.put("min_translog_gen", 1L);
        metadata.put("generation_to_primary_term", genToTermMap);

        return Map.of("metadata__translog1", metadata);
    }
}
