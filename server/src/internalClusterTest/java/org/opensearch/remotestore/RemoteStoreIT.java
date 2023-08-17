/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.recovery.RecoveryResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.hamcrest.MatcherAssert;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.opensearch.index.shard.RemoteStoreRefreshListener.LAST_N_METADATA_FILES_TO_KEEP;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class RemoteStoreIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Before
    public void setup() {
        setupRepo();
    }

    @Override
    public Settings indexSettings() {
        return remoteStoreIndexSettings(0);
    }

    private void testPeerRecovery(int numberOfIterations, boolean invokeFlush) throws Exception {
        internalCluster().startDataOnlyNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, INDEX_NAME);

        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            .get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        String replicaNodeName = replicaNodeName(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(replicaNodeName).prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS)),
            30,
            TimeUnit.SECONDS
        );

        RecoveryResponse recoveryResponse = client(replicaNodeName).admin().indices().prepareRecoveries().get();

        Optional<RecoveryState> recoverySource = recoveryResponse.shardRecoveryStates()
            .get(INDEX_NAME)
            .stream()
            .filter(rs -> rs.getRecoverySource().getType() == RecoverySource.Type.PEER)
            .findFirst();
        assertFalse(recoverySource.isEmpty());
        // segments_N file is copied to new replica
        assertEquals(1, recoverySource.get().getIndex().recoveredFileCount());

        IndexResponse response = indexSingleDoc(INDEX_NAME);
        assertEquals(indexStats.get(MAX_SEQ_NO_TOTAL) + 1, response.getSeqNo());
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertHitCount(client(replicaNodeName).prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1),
            30,
            TimeUnit.SECONDS
        );
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogNoDataFlush() throws Exception {
        testPeerRecovery(1, true);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogFlush() throws Exception {
        testPeerRecovery(randomIntBetween(2, 5), true);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogNoDataRefresh() throws Exception {
        testPeerRecovery(1, false);
    }

    public void testPeerRecoveryWithRemoteStoreAndRemoteTranslogRefresh() throws Exception {
        testPeerRecovery(randomIntBetween(2, 5), false);
    }

    private void verifyRemoteStoreCleanup() throws Exception {
        internalCluster().startDataOnlyNodes(3);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));

        indexData(5, randomBoolean(), INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID);
        assertTrue(getFileCount(indexPath) > 0);
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        // Delete is async. Give time for it
        assertBusy(() -> {
            try {
                assertThat(getFileCount(indexPath), comparesEqualTo(0));
            } catch (Exception e) {}
        }, 30, TimeUnit.SECONDS);
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/9327")
    public void testRemoteTranslogCleanup() throws Exception {
        verifyRemoteStoreCleanup();
    }

    public void testStaleCommitDeletionWithInvokeFlush() throws Exception {
        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, true, INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID, "/0/segments/metadata");
        // Delete is async.
        assertBusy(() -> {
            int actualFileCount = getFileCount(indexPath);
            if (numberOfIterations <= LAST_N_METADATA_FILES_TO_KEEP) {
                MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
            } else {
                // As delete is async its possible that the file gets created before the deletion or after
                // deletion.
                MatcherAssert.assertThat(
                    actualFileCount,
                    is(oneOf(LAST_N_METADATA_FILES_TO_KEEP - 1, LAST_N_METADATA_FILES_TO_KEEP, LAST_N_METADATA_FILES_TO_KEEP + 1))
                );
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testStaleCommitDeletionWithoutInvokeFlush() throws Exception {
        internalCluster().startDataOnlyNodes(1);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1, 10000l, -1));
        int numberOfIterations = randomIntBetween(5, 15);
        indexData(numberOfIterations, false, INDEX_NAME);
        String indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        Path indexPath = Path.of(String.valueOf(absolutePath), indexUUID, "/0/segments/metadata");
        int actualFileCount = getFileCount(indexPath);
        // We also allow (numberOfIterations + 1) as index creation also triggers refresh.
        MatcherAssert.assertThat(actualFileCount, is(oneOf(numberOfIterations - 1, numberOfIterations, numberOfIterations + 1)));
    }
}
