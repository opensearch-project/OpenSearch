/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;

import java.nio.file.Path;

public class RemoteStorePeerRecoverySourceHandlerTests extends OpenSearchIndexLevelReplicationTestCase {

    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, "true")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY, "translog-repo")
        .put(IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.getKey(), "100ms")
        .build();

    @Before
    public void setup() {
        // Todo: Remove feature flag once remote store integration with segrep goes GA
        FeatureFlags.initializeFeatureFlags(
            Settings.builder().put(FeatureFlags.SEGMENT_REPLICATION_EXPERIMENTAL_SETTING.getKey(), "true").build()
        );
    }

    public void testReplicaShardRecoveryUptoLastFlushedCommit() throws Exception {
        final Path remoteDir = createTempDir();
        final String indexMapping = "{ \"" + MapperService.SINGLE_MAPPING_NAME + "\": {} }";
        try (ReplicationGroup shards = createGroup(0, settings, indexMapping, new NRTReplicationEngineFactory(), remoteDir)) {

            // Step1 - Start primary, index docs and flush
            shards.startPrimary();
            final IndexShard primary = shards.getPrimary();
            int numDocs = shards.indexDocs(randomIntBetween(10, 20));
            logger.info("--> Index numDocs {} and flush", numDocs);
            shards.flush();

            // Step 2 - Start replica for recovery to happen, check both has same number of docs
            final IndexShard replica1 = shards.addReplica(remoteDir);
            logger.info("--> Added and started replica {}", replica1.routingEntry());
            shards.startAll();
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 3 - Index more docs, run segment replication, check both have same number of docs
            int moreDocs = shards.indexDocs(randomIntBetween(10, 20));
            primary.refresh("test");
            logger.info("--> Index more docs {} and replicate segments", moreDocs);
            replicateSegments(primary, shards.getReplicas());
            assertEquals(getDocIdAndSeqNos(primary), getDocIdAndSeqNos(replica1));

            // Step 4 - Check both shard has expected number of doc count
            assertDocCount(primary, numDocs + moreDocs);
            assertDocCount(replica1, numDocs + moreDocs);

            // Step 5 - Check retention lease does not exist for the replica shard
            assertEquals(1, primary.getRetentionLeases().leases().size());
            assertFalse(primary.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replica1.routingEntry())));

            // Step 6 - Start new replica, recovery happens, and check that new replica has all docs
            final IndexShard replica2 = shards.addReplica(remoteDir);
            logger.info("--> Added and started replica {}", replica2.routingEntry());
            shards.startAll();
            shards.assertAllEqual(numDocs + moreDocs);

            // Step 7 - Check retention lease does not exist for the replica shard
            assertEquals(1, primary.getRetentionLeases().leases().size());
            assertFalse(primary.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(replica2.routingEntry())));
        }
    }
}
