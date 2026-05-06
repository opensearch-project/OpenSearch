/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;

/**
 * ITs for DFA peer recovery scenarios: fresh replica allocation under load and
 * catalog preservation through file-based peer recovery.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwarePeerRecoveryIT extends DataFormatAwareReplicationBaseIT {

    public void testPeerRecoveryOfFreshReplicaUnderIndexingLoad() throws Exception {
        // Start cluster with 0 replicas initially
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.setIgnoreIndexingFailures(true);
            indexer.start(-1);
            waitForIndexerDocs(200, indexer);

            // Increase replica count to 1 — triggers peer recovery under load
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
                .get();

            indexer.continueIndexing(200);

            ensureGreen(INDEX_NAME);

            indexer.stopAndAwaitStopped();

            assertNoDataLoss(indexer, INDEX_NAME);
            assertCatalogSnapshotsConverged(INDEX_NAME);
        }
    }

    public void testPeerRecoveryPreservesDFACatalogInCommitData() throws Exception {
        createDfaIndex(1);

        // Index + flush twice to create multiple generations
        indexDocs(100);
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        indexDocs(100);
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Force file-based peer recovery: stop replica, start new node
        String replicaNode = replicaNodeNames().get(0);
        internalCluster().stopRandomNode(s -> replicaNode.equals(s));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // After peer recovery completes, find the ACTUAL new replica node from cluster state.
        // (Starting a new data node does not guarantee that node holds the replica — allocation
        // decides; we must query cluster state for the real placement.)
        String newReplicaNode = replicaNodeNames().get(0);

        // Verify the recovered replica has a DataformatAwareCatalogSnapshot
        IndexShard recovered = getIndexShard(newReplicaNode, INDEX_NAME);
        try (GatedCloseable<CatalogSnapshot> closeable = recovered.getCatalogSnapshot()) {
            CatalogSnapshot snap = closeable.get();
            assertThat(snap, instanceOf(DataformatAwareCatalogSnapshot.class));
        }

        // Verify catalog snapshot key is present in commit user data
        Map<String, String> userData = recovered.commitStats().getUserData();
        assertTrue(
            "commit user data must contain catalog snapshot key, got keys: " + userData.keySet(),
            userData.containsKey(CatalogSnapshot.CATALOG_SNAPSHOT_KEY)
        );

        assertCatalogSnapshotsConverged(INDEX_NAME);
    }
}
