/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 3)
public class RemoteStoreForceMergeIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-test-idx-1";
    private static final String TOTAL_OPERATIONS = "total-operations";
    private static final String MAX_SEQ_NO_TOTAL = "max-seq-no-total";

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

    private Map<String, Long> indexData(int numberOfIterations, boolean invokeFlush, boolean flushAfterMerge, long deletedDocs) {
        long totalOperations = 0;
        long maxSeqNo = -1;
        List<IndexResponse> indexResponseList = new ArrayList<>();
        for (int i = 0; i < numberOfIterations; i++) {
            int numberOfOperations = randomIntBetween(20, 50);
            for (int j = 0; j < numberOfOperations; j++) {
                IndexResponse response = indexSingleDoc(INDEX_NAME);
                maxSeqNo = response.getSeqNo();
                indexResponseList.add(response);
            }
            totalOperations += numberOfOperations;
            if (invokeFlush) {
                flush(INDEX_NAME);
            } else {
                refresh(INDEX_NAME);
            }
        }
        if (deletedDocs == -1) {
            deletedDocs = totalOperations;
        }
        int length = indexResponseList.size();
        for (int j = 0; j < deletedDocs; j++) {
            maxSeqNo = client().prepareDelete().setIndex(INDEX_NAME).setId(indexResponseList.get(length - j - 1).getId()).get().getSeqNo();
        }
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(flushAfterMerge).get();
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), totalOperations - deletedDocs);
        Map<String, Long> indexingStats = new HashMap<>();
        indexingStats.put(TOTAL_OPERATIONS, totalOperations);
        indexingStats.put(MAX_SEQ_NO_TOTAL, maxSeqNo);
        return indexingStats;
    }

    private void verifyRestoredData(Map<String, Long> indexStats, long deletedDocs) {
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) - deletedDocs);
        IndexResponse response = indexSingleDoc(INDEX_NAME);
        assertEquals(indexStats.get(MAX_SEQ_NO_TOTAL) + 1, response.getSeqNo());
        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), indexStats.get(TOTAL_OPERATIONS) + 1 - deletedDocs);
    }

    private void testRestoreWithMergeFlow(int numberOfIterations, boolean invokeFlush, boolean flushAfterMerge, long deletedDocs)
        throws IOException {
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        Map<String, Long> indexStats = indexData(numberOfIterations, invokeFlush, flushAfterMerge, deletedDocs);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName(INDEX_NAME)));

        boolean restoreAllShards = randomBoolean();
        if (restoreAllShards) {
            assertAcked(client().admin().indices().prepareClose(INDEX_NAME));
        }
        client().admin()
            .cluster()
            .restoreRemoteStore(
                new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(restoreAllShards),
                PlainActionFuture.newFuture()
            );
        ensureGreen(INDEX_NAME);

        if (deletedDocs == -1) {
            verifyRestoredData(indexStats, indexStats.get(TOTAL_OPERATIONS));
        } else {
            verifyRestoredData(indexStats, deletedDocs);
        }
    }

    // Following integ tests use randomBoolean to control the number of integ tests. If we use the separate
    // values for each of the flags, number of integ tests become 16 in comparison to current 2.
    // We have run all the 16 tests on local and they run fine.
    public void testRestoreForceMergeSingleIteration() throws IOException {
        boolean invokeFLush = randomBoolean();
        boolean flushAfterMerge = randomBoolean();
        testRestoreWithMergeFlow(1, invokeFLush, flushAfterMerge, randomIntBetween(0, 10));
    }

    public void testRestoreForceMergeMultipleIterations() throws IOException {
        boolean invokeFLush = randomBoolean();
        boolean flushAfterMerge = randomBoolean();
        testRestoreWithMergeFlow(randomIntBetween(2, 5), invokeFLush, flushAfterMerge, randomIntBetween(0, 10));
    }

    public void testRestoreForceMergeMultipleIterationsDeleteAll() throws IOException {
        boolean invokeFLush = randomBoolean();
        boolean flushAfterMerge = randomBoolean();
        testRestoreWithMergeFlow(randomIntBetween(2, 3), invokeFLush, flushAfterMerge, -1);
    }
}
