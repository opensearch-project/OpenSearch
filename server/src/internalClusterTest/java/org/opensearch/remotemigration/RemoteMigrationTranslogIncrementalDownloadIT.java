/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotemigration;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.RemoteFsTimestampAwareTranslog;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * A shard that starts life with a local (docrep) translog is migrated to a remote node by relocation: the target
 * seeds the remote store with an empty translog and then streams the source's operations through a remote-enabled
 * writer, so every generation it uploads carries a footer even though the docrep node never wrote one. A later
 * remote-to-remote relocation must be able to reuse those generations instead of downloading them a second time.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteMigrationTranslogIncrementalDownloadIT extends MigrationBaseTestCase {

    private static final String INDEX_NAME = "test";

    @Override
    protected int maximumNumberOfShards() {
        return 1;
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 0;
    }

    private void indexDocsAsSeparateGenerations(int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex(INDEX_NAME).setSource("field", "value-" + i).get();
        }
    }

    private void relocatePrimary(String from, String to) {
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand(INDEX_NAME, 0, from, to)).execute().actionGet();
        waitForRelocation();
        assertEquals(to, primaryNodeName(INDEX_NAME));
    }

    public void testRemoteToRemoteRelocationAfterMigrationReusesSeededGenerations() throws Exception {
        List<String> docRepNodes = internalCluster().startNodes(2);
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings()).setMapping("field", "type=keyword").get();
        ensureGreen(INDEX_NAME);
        // Written by a docrep node: local translog only, no footers, nothing uploaded.
        int docRepDocs = randomIntBetween(5, 15);
        indexDocsAsSeparateGenerations(docRepDocs);
        String docRepPrimary = primaryNodeName(INDEX_NAME);
        assertTrue(docRepNodes.contains(docRepPrimary));

        setAddRemote(true);
        String remoteNode = internalCluster().startNode();
        String remoteNode2 = internalCluster().startNode();
        internalCluster().validateClusterFormed();
        updateSettingsRequest.persistentSettings(Settings.builder().put(MIGRATION_DIRECTION_SETTING.getKey(), "remote_store"));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // docrep -> remote: the remote store is wiped, seeded with an empty translog, and the docrep operations are
        // replayed into a remote-enabled writer. The remote store only ever sees footer'd generations for this data.
        relocatePrimary(docRepPrimary, remoteNode);
        int remoteDocs = randomIntBetween(5, 15);
        indexDocsAsSeparateGenerations(remoteDocs);

        // remote -> remote: the target downloads the translog twice into the same directory while opening its
        // engine; the second pass must reuse what the first one wrote.
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(
                LogManager.getLogger(IndexShard.class),
                LogManager.getLogger(RemoteFsTranslog.class),
                LogManager.getLogger(RemoteFsTimestampAwareTranslog.class)
            )
        ) {
            appender.addExpectation(
                new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
                    "seeded translog generations reused on remote-to-remote relocation",
                    "org.opensearch.index",
                    Level.INFO,
                    ".*generations already present locally=[1-9][0-9]*"
                )
            );
            relocatePrimary(remoteNode, remoteNode2);
            appender.assertAllExpectationsMatched();
        }

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docRepDocs + remoteDocs);
        indexDocsAsSeparateGenerations(3);
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), docRepDocs + remoteDocs + 3);
    }
}
