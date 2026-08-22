/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;

/**
 * Verifies that {@link org.opensearch.index.engine.InternalEngine} drives the translog byte tracking that backs
 * {@code index.translog.flush_threshold_size} on remote translog backed shards, in particular that a successful index
 * commit releases the tracked bytes.
 */
public class RemoteStoreTranslogBytesFlushTests extends IndexShardTestCase {

    private static final int FLUSH_THRESHOLD_BYTES = 512;

    private boolean trackTranslogBytes = true;

    @Override
    protected RemoteStoreSettings remoteStoreSettings() {
        Settings settings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_TRACK_BYTES_SINCE_LAST_COMMIT_SETTING.getKey(), trackTranslogBytes)
            .build();
        return new RemoteStoreSettings(settings, new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public void testSuccessfulCommitReleasesTrackedTranslogBytes() throws Exception {
        trackTranslogBytes = true;
        IndexShard shard = newStartedShard(true, remoteBackedIndexSettings());
        try {
            assertTrue(getTranslog(shard) instanceof RemoteFsTranslog);

            indexUntilFlushThresholdIsCrossed(shard);

            /*
             * The threshold has been crossed, so a commit has to bring the trigger back down. The translog files still
             * hold every one of those bytes, so this only passes when the commit released them from the tracker, which
             * is the wiring under test.
             */
            flushShard(shard);
            assertFalse(shard.shouldPeriodicallyFlush());
            assertEquals(shard.getProcessedLocalCheckpoint(), lastCommittedLocalCheckpoint(shard));

            // A single small operation must not put the shard back over the threshold.
            indexDoc(shard, "_doc", "after-flush", "{\"f\":\"v\"}");
            assertFalse(shard.shouldPeriodicallyFlush());

            // Bytes accumulate again from the commit, so the trigger fires a second time.
            indexUntilFlushThresholdIsCrossed(shard);
            flushShard(shard);
            assertFalse(shard.shouldPeriodicallyFlush());
        } finally {
            closeShards(shard);
        }
    }

    /**
     * With the setting off the legacy size computation decides when to flush. The commit hooks must stay out of the way
     * entirely, so a flush still has to produce a commit that carries the shard's processed checkpoint. Note that the
     * legacy trigger is deliberately not asserted here: it is driven by the remote retention boundary, which only moves
     * once segments are uploaded, and that is covered end to end by the integration tests.
     */
    public void testDisabledTrackingStillCommitsOnFlush() throws Exception {
        trackTranslogBytes = false;
        IndexShard shard = newStartedShard(true, remoteBackedIndexSettings());
        try {
            assertTrue(getTranslog(shard) instanceof RemoteFsTranslog);

            indexUntilFlushThresholdIsCrossed(shard);
            flushShard(shard);

            assertEquals(shard.getProcessedLocalCheckpoint(), lastCommittedLocalCheckpoint(shard));
        } finally {
            closeShards(shard);
        }
    }

    private long lastCommittedLocalCheckpoint(IndexShard shard) {
        return Long.parseLong(shard.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
    }

    /**
     * Indexes until the shard reports that a periodic flush is due. Operations are indexed one at a time because the
     * shard may flush on its own once the threshold is crossed.
     */
    private void indexUntilFlushThresholdIsCrossed(IndexShard shard) throws Exception {
        String payload = randomAlphaOfLength(FLUSH_THRESHOLD_BYTES);
        for (int i = 0; i < 100; i++) {
            if (shard.shouldPeriodicallyFlush()) {
                return;
            }
            indexDoc(shard, "_doc", "id-" + shard.getLocalCheckpoint() + "-" + i, "{\"payload\":\"" + payload + "\"}");
        }
        assertTrue("shard did not reach the translog flush threshold", shard.shouldPeriodicallyFlush());
    }

    private Settings remoteBackedIndexSettings() throws IOException {
        return Settings.builder()
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
            .put(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY, "seg-repo")
            .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "txlog-repo")
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), FLUSH_THRESHOLD_BYTES + "b")
            .build();
    }
}
