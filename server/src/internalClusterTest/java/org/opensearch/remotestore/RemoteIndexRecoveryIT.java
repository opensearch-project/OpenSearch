/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.recovery.IndexRecoveryIT;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;

import static org.opensearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING;
import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.remoteStoreClusterSettings;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteIndexRecoveryIT extends IndexRecoveryIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";

    protected Path repositoryPath;

    @Before
    public void setup() {
        repositoryPath = randomRepoPath().toAbsolutePath();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, repositoryPath))
            .build();
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "300s")
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    @Override
    public void slowDownRecovery(ByteSizeValue shardSize) {
        logger.info("--> shardSize: " + shardSize);
        long chunkSize = Math.max(1, shardSize.getBytes() / 50);
        assertTrue(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        // one chunk per sec..
                        .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), chunkSize, ByteSizeUnit.BYTES)
                        // small chunks
                        .put(INDICES_RECOVERY_CHUNK_SIZE_SETTING.getKey(), new ByteSizeValue(chunkSize, ByteSizeUnit.BYTES))
                )
                .get()
                .isAcknowledged()
        );
    }

    @After
    public void teardown() {
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }

    @Override
    protected Matcher<Long> getMatcherForThrottling(long value) {
        return Matchers.greaterThanOrEqualTo(value);
    }

    @Override
    protected int numDocs() {
        return randomIntBetween(100, 200);
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseMissing() {
        // Retention lease based tests not applicable for remote store;
    }

    @Override
    public void testPeerRecoveryTrimsLocalTranslog() {
        // Peer recovery usecase not valid for remote enabled indices
    }

    @Override
    public void testHistoryRetention() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfOperationsBasedRecoveryWouldBeUnreasonable() {
        // History retention not applicable for remote store
    }

    @Override
    public void testUsesFileBasedRecoveryIfRetentionLeaseAheadOfGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testRecoverLocallyUpToGlobalCheckpoint() {
        // History retention not applicable for remote store
    }

    @Override
    public void testCancelNewShardRecoveryAndUsesExistingShardCopy() {
        // History retention not applicable for remote store
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testReservesBytesDuringPeerRecoveryPhaseOne() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testAllocateEmptyPrimaryResetsGlobalCheckpoint() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDoesNotCopyOperationsInSafeCommit() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testRepeatedRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDisconnectsWhileRecovering() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testTransientErrorsDuringRecoveryAreRetried() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDoNotInfinitelyWaitForMapping() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testDisconnectsDuringRecovery() {

    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/8919")
    @Override
    public void testReplicaRecovery() {

    }
}
