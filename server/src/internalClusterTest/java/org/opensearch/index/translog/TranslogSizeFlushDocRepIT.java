/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.index.shard.IndexShardTestCase.getTranslog;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Asserts that tracking remote translog bytes since the last commit leaves document replication indices alone. Those
 * shards keep a local translog and must reach {@code index.translog.flush_threshold_size} exactly as before, whether the
 * cluster setting is on or off.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TranslogSizeFlushDocRepIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "docrep-translog-flush-idx";
    private static final int DOC_SIZE_IN_BYTES = 4 * 1024;

    public void testDocumentReplicationFlushUnchangedWhenTrackingEnabled() throws Exception {
        assertDocRepSizeBasedFlush(true);
    }

    public void testDocumentReplicationFlushUnchangedWhenTrackingDisabled() throws Exception {
        assertDocRepSizeBasedFlush(false);
    }

    private void assertDocRepSizeBasedFlush(boolean trackBytesSinceLastCommit) throws Exception {
        Settings nodeSettings = Settings.builder().put(IndexingMemoryController.SHARD_INACTIVE_TIME_SETTING.getKey(), "1h").build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNode = internalCluster().startDataOnlyNode(nodeSettings);

        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder()
                        .put(
                            RemoteStoreSettings.CLUSTER_REMOTE_TRANSLOG_TRACK_BYTES_SINCE_LAST_COMMIT_SETTING.getKey(),
                            trackBytesSinceLastCommit
                        )
                )
                .get()
        );

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "32kb")
                .put(IndexSettings.INDEX_PERIODIC_FLUSH_INTERVAL_SETTING.getKey(), "-1")
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
                .build()
        );
        ensureGreen(INDEX_NAME);

        IndexShard indexShard = getIndexShard(dataNode, INDEX_NAME);
        // The byte tracking is scoped to remote translog backed shards, so this shard must not even be a candidate.
        assertFalse(getTranslog(indexShard) instanceof RemoteFsTranslog);
        long initialPeriodicFlushes = indexShard.flushStats().getPeriodic();
        long initialCommitCheckpoint = getLastCommittedLocalCheckpoint(indexShard);
        assertFalse(indexShard.shouldPeriodicallyFlush());

        // Write past the 32kb threshold. Refreshing on every write is what suppresses the flush on a remote translog,
        // so doing the same here keeps the two setups comparable.
        String payload = randomAlphaOfLength(DOC_SIZE_IN_BYTES);
        for (int i = 0; i < 12; i++) {
            client(dataNode).prepareIndex(INDEX_NAME)
                .setId(Integer.toString(i))
                .setSource("payload", payload)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        }

        assertBusy(() -> {
            assertThat(indexShard.flushStats().getPeriodic(), greaterThan(initialPeriodicFlushes));
            assertThat(getLastCommittedLocalCheckpoint(indexShard), greaterThan(initialCommitCheckpoint));
        }, 30, TimeUnit.SECONDS);
    }

    private IndexShard getIndexShard(String node, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexService(index);
        assertNotNull(indexService);
        return indexService.getShard(0);
    }

    private long getLastCommittedLocalCheckpoint(IndexShard indexShard) {
        return Long.parseLong(indexShard.commitStats().getUserData().get(SequenceNumbers.LOCAL_CHECKPOINT_KEY));
    }
}
