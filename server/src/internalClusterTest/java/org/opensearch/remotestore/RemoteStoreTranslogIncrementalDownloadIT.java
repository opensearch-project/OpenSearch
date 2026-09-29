/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.RemoteFsTimestampAwareTranslog;
import org.opensearch.index.translog.RemoteFsTranslog;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Exercises the incremental remote translog download end to end. A remote-store primary opens its engine by
 * downloading the translog twice into the same directory (once in {@code IndexShard#innerOpenEngineAndTranslog},
 * once more from the {@code RemoteFsTranslog} constructor), and a promoted replica does the same in
 * {@code IndexShard#resetEngineToGlobalCheckpoint}. The second pass must find the generations the first pass wrote
 * and reuse every one that carries a footer instead of fetching it again.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreTranslogIncrementalDownloadIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";

    /**
     * Each request-durability write rolls and uploads a generation, so this leaves the remote store with
     * {@code numDocs} closed generations that carry a footer (plus the footer-less empty initial one).
     */
    private void indexDocsAsSeparateGenerations(int numDocs) {
        for (int i = 0; i < numDocs; i++) {
            indexSingleDoc(INDEX_NAME);
        }
    }

    /**
     * Logged by the second download pass when at least one generation was found locally and reused. The first pass
     * always lands in a directory holding no reusable generation and logs {@code locally=0}, which this does not match.
     */
    private static MockLogAppender.PatternSeenWithLoggerPrefixExpectation generationsReusedExpectation() {
        return new MockLogAppender.PatternSeenWithLoggerPrefixExpectation(
            "translog generations reused on second download pass",
            "org.opensearch.index",
            Level.INFO,
            ".*generations already present locally=[1-9][0-9]*"
        );
    }

    private static MockLogAppender downloadLogAppender() throws IllegalAccessException {
        // The first pass logs through the shard's logger, the second through the translog's; listen on both.
        return MockLogAppender.createForLoggers(
            LogManager.getLogger(IndexShard.class),
            LogManager.getLogger(RemoteFsTranslog.class),
            LogManager.getLogger(RemoteFsTimestampAwareTranslog.class)
        );
    }

    public void testPrimaryRelocationReusesLocallyPresentGenerations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String sourceNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        int numDocs = randomIntBetween(10, 30);
        indexDocsAsSeparateGenerations(numDocs);
        assertEquals(sourceNode, primaryNodeName(INDEX_NAME));

        String targetNode = internalCluster().startDataOnlyNode();
        try (MockLogAppender appender = downloadLogAppender()) {
            appender.addExpectation(generationsReusedExpectation());
            client().admin()
                .cluster()
                .prepareReroute()
                .add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode))
                .execute()
                .actionGet();
            ensureGreen(INDEX_NAME);
            assertEquals(targetNode, primaryNodeName(INDEX_NAME));
            appender.assertAllExpectationsMatched();
        }

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), numDocs);
        // The relocated primary keeps working against the reused generations.
        indexDocsAsSeparateGenerations(5);
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), numDocs + 5);
    }

    public void testReplicaPromotionReusesLocallyPresentGenerations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));
        ensureGreen(INDEX_NAME);
        int numDocs = randomIntBetween(10, 30);
        indexDocsAsSeparateGenerations(numDocs);
        String primaryNode = primaryNodeName(INDEX_NAME);
        String replicaNode = replicaNodeName(INDEX_NAME);

        try (MockLogAppender appender = downloadLogAppender()) {
            appender.addExpectation(generationsReusedExpectation());
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
            ensureYellowAndNoInitializingShards(INDEX_NAME);
            assertEquals(replicaNode, primaryNodeName(INDEX_NAME));
            // The promoted replica resets its engine (and downloads the translog) asynchronously after the routing
            // table already names it primary, so give that reset time to run.
            assertBusy(appender::assertAllExpectationsMatched, 30, TimeUnit.SECONDS);
        }

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), numDocs);
        indexDocsAsSeparateGenerations(5);
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), numDocs + 5);
    }
}
