/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataResponse;
import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreShardMetadata;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * The remote store metadata API has to answer for a shard with remote segments and no remote translog, a combination
 * the API was written before and only ever saw as a misconfiguration.
 */
public class SegmentsOnlyMetadataIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-metadata-idx";

    /**
     * The translog section has to come back empty rather than failing the shard. An unset repository setting reads
     * back as the empty string, so a shard is only reported on at all if the API tells that apart from a repository
     * that is set, and the segment metadata is lost along with it if it does not.
     */
    public void testMetadataIsReportedWithoutATranslogRepository() throws Exception {
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);
        indexDocs(INDEX_NAME, 0, 20);
        refresh(INDEX_NAME);

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();
        assertEquals("no shard may fail: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        assertEquals(1, response.getSuccessfulShards());

        List<RemoteStoreShardMetadata> shards = response.groupByIndexAndShards().get(INDEX_NAME).get(0);
        assertFalse(shards.isEmpty());
        for (RemoteStoreShardMetadata metadata : shards) {
            Map<String, Map<String, Object>> segmentFiles = metadata.getSegmentMetadataFiles();
            assertNotNull(segmentFiles);
            assertFalse("the segments are remote backed, so they must be described", segmentFiles.isEmpty());
            assertNotNull(metadata.getLatestSegmentMetadataFileName());

            assertEquals("a local translog uploads no metadata", Map.of(), metadata.getTranslogMetadataFiles());
            assertNull(metadata.getLatestTranslogMetadataFileName());
        }
    }

    /** A shard with nothing uploaded yet still has to be described rather than failed. */
    public void testMetadataReportedBeforeFirstRefresh() throws Exception {
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);

        RemoteStoreMetadataResponse response = client().admin().cluster().prepareRemoteStoreMetadata(INDEX_NAME, null).get();
        assertEquals("no shard may fail: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
        for (RemoteStoreShardMetadata metadata : response.groupByIndexAndShards().get(INDEX_NAME).get(0)) {
            assertEquals(Map.of(), metadata.getTranslogMetadataFiles());
        }
    }
}
