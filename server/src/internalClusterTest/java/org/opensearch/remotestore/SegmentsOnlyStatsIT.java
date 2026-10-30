/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStats;
import org.opensearch.action.admin.cluster.remotestore.stats.RemoteStoreStatsResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.index.remote.RemoteSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.RemoteTranslogStats;

import java.util.Arrays;

/**
 * The stats APIs have to describe a shard whose segments are remote backed and whose translog is not, which is a
 * combination neither document replication nor a fully remote backed cluster produces.
 */
public class SegmentsOnlyStatsIT extends SegmentsOnlyBaseIntegTestCase {

    private static final String INDEX_NAME = "segments-only-stats-idx";

    /**
     * Nothing ever uploads a segments_only translog, so every upload counter must stay at zero. The counters are
     * reported rather than omitted, which is what a document replication shard does too, so the check here is that
     * they describe an inactive uploader rather than that the block is absent.
     */
    public void testTheLocalTranslogIsNeverUploaded() throws Exception {
        String dataNode = startCluster(1).get(0);
        createSegmentsOnlyIndex(INDEX_NAME, 0);
        indexDocs(INDEX_NAME, 0, 20);
        flush(INDEX_NAME);

        IndexShard shard = shardOn(dataNode, INDEX_NAME);
        assertSegmentsOnly(shard);

        RemoteTranslogStats uploads = shard.translogStats().getRemoteTranslogStats();
        assertEquals("a segments_only translog must never start an upload", 0L, uploads.getTotalUploadsStarted());
        assertEquals(0L, uploads.getTotalUploadsSucceeded());
        assertEquals(0L, uploads.getTotalUploadsFailed());
        assertEquals(0L, uploads.getUploadBytesStarted());

        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).get();
        assertEquals(0, stats.getFailedShards());
        assertEquals(0L, stats.getTotal().getTranslog().getRemoteTranslogStats().getTotalUploadsStarted());
    }

    /** The shard level stats APIs must all answer for such a shard rather than failing the shard they describe. */
    public void testStatsApisDescribeASegmentsOnlyShard() throws Exception {
        startCluster(2);
        createSegmentsOnlyIndex(INDEX_NAME, 1);
        indexDocs(INDEX_NAME, 0, 20);
        refresh(INDEX_NAME);

        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).get();
        assertEquals(0, stats.getFailedShards());
        assertNotNull(stats.getTotal().getTranslog());
        assertNotNull(stats.getTotal().getSegments());
        assertNotNull(stats.getTotal().getDocs());

        assertEquals(0, client().admin().indices().prepareSegments(INDEX_NAME).get().getFailedShards());
        assertEquals(0, client().admin().indices().prepareRecoveries(INDEX_NAME).get().getFailedShards());
    }

    /**
     * {@code _remotestore/stats} is the only view of the segment uploader, and here it has to pair a working uploader
     * with a translog tracker nothing ever feeds. A shard is described at all only if the API asks after the two
     * halves separately rather than assuming a remote backed shard has both.
     */
    public void testRemoteStoreStatsWithIdleTranslog() throws Exception {
        startCluster(1);
        createSegmentsOnlyIndex(INDEX_NAME, 0);
        indexDocs(INDEX_NAME, 0, 20);
        refresh(INDEX_NAME);

        assertBusy(() -> {
            RemoteStoreStatsResponse response = client().admin().cluster().prepareRemoteStoreStats(INDEX_NAME, "0").get();
            assertEquals("no shard may fail: " + Arrays.toString(response.getShardFailures()), 0, response.getFailedShards());
            assertEquals(1, response.getSuccessfulShards());

            RemoteStoreStats stats = response.getRemoteStoreStats()[0];
            RemoteSegmentTransferTracker.Stats segments = stats.getSegmentStats();
            assertTrue("the segments are remote backed, so they must be uploaded", segments.totalUploadsSucceeded > 0);
            assertEquals(0, segments.totalUploadsFailed);
            assertEquals("the uploader must catch up with the local refresh", 0, segments.bytesLag);

            assertEquals("a local translog is never uploaded", 0, stats.getTranslogStats().totalUploadsStarted);
        });
    }
}
