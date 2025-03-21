/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.snapshots;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStage;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opensearch.snapshots.SnapshotsService.MAX_SHARDS_ALLOWED_IN_STATUS_API;
import static org.opensearch.test.OpenSearchIntegTestCase.resolvePath;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SnapshotStatusApisIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0) // We have tests that check by-timestamp order
            .build();
    }

    public void testStatusApiConsistency() {
        createRepository("test-repo", "fs");

        createIndex("test-idx-1", "test-idx-2", "test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
            index("test-idx-2", "_doc", Integer.toString(i), "foo", "baz" + i);
            index("test-idx-3", "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();

        createFullSnapshot("test-repo", "test-snap");

        List<SnapshotInfo> snapshotInfos = clusterAdmin().prepareGetSnapshots("test-repo").get().getSnapshots();
        assertThat(snapshotInfos.size(), equalTo(1));
        SnapshotInfo snapshotInfo = snapshotInfos.get(0);
        assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.version(), equalTo(Version.CURRENT));

        final SnapshotStatus snapshotStatus = getSnapshotStatus("test-repo", "test-snap");
        assertEquals(snapshotStatus.getStats().getStartTime(), snapshotInfo.startTime());
        assertEquals(snapshotStatus.getStats().getTime(), snapshotInfo.endTime() - snapshotInfo.startTime());
    }

    public void testStatusAPICallForShallowCopySnapshot() throws Exception {
        disableRepoConsistencyCheck("Remote store repository is being used for the test");
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "fs", snapshotRepoSettingsForShallowCopy());

        final String indexName = "index-1";
        createIndex(indexName);
        ensureGreen();
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String snapshot = "snapshot";
        createFullSnapshot(snapshotRepoName, snapshot);

        assertBusy(() -> {
            final SnapshotStatus snapshotStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(snapshotRepoName)
                .setSnapshots(snapshot)
                .execute()
                .actionGet()
                .getSnapshots()
                .get(0);
            assertThat(snapshotStatus.getState(), is(SnapshotsInProgress.State.SUCCESS));

            final SnapshotIndexShardStatus snapshotShardState = stateFirstShard(snapshotStatus, indexName);
            assertThat(snapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardState.getStats().getTotalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getTotalSize(), greaterThan(0L));
            assertThat(snapshotShardState.getStats().getIncrementalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getIncrementalSize(), greaterThan(0L));
        }, 20, TimeUnit.SECONDS);
    }

    public void testStatusAPICallInProgressSnapshot() throws Exception {
        createRepository("test-repo", "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot("test-repo", "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode("test-repo", TimeValue.timeValueMinutes(1));
        awaitNumberOfSnapshotsInProgress(1);
        assertEquals(
            SnapshotsInProgress.State.STARTED,
            client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").get().getSnapshots().get(0).getState()
        );

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes("test-repo");

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testExceptionOnMissingSnapBlob() throws IOException {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");
        logger.info("--> delete snap-${uuid}.dat file for this snapshot to simulate concurrent delete");
        IOUtils.rm(repoPath.resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat"));

        expectThrows(
            SnapshotMissingException.class,
            () -> client().admin().cluster().getSnapshots(new GetSnapshotsRequest("test-repo", new String[] { "test-snap" })).actionGet()
        );
    }

    public void testExceptionOnMissingShardLevelSnapBlob() throws Exception {
        disableRepoConsistencyCheck("This test intentionally corrupts the repository");

        final Path repoPath = randomRepoPath();
        createRepository("test-repo", "fs", repoPath);

        createIndex("test-idx-1");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index("test-idx-1", "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final SnapshotInfo snapshotInfo = createFullSnapshot("test-repo", "test-snap");

        logger.info("--> delete shard-level snap-${uuid}.dat file for one shard in this snapshot to simulate concurrent delete");
        IndexId indexId = getRepositoryData("test-repo").resolveIndexId(snapshotInfo.indices().get(0));
        IOUtils.rm(
            repoPath.resolve(resolvePath(indexId, "0"))
                .resolve(BlobStoreRepository.SNAPSHOT_PREFIX + snapshotInfo.snapshotId().getUUID() + ".dat")
        );
        assertBusy(() -> {
            expectThrows(
                SnapshotMissingException.class,
                () -> client().admin().cluster().prepareSnapshotStatus("test-repo").setSnapshots("test-snap").execute().actionGet()
            );
        }, 20, TimeUnit.SECONDS);
    }

    public void testGetSnapshotsWithoutIndices() throws Exception {
        createRepository("test-repo", "fs");

        logger.info("--> snapshot");
        final SnapshotInfo snapshotInfo = assertSuccessful(
            client().admin().cluster().prepareCreateSnapshot("test-repo", "test-snap").setIndices().setWaitForCompletion(true).execute()
        );
        assertThat(snapshotInfo.totalShards(), is(0));

        logger.info("--> verify that snapshot without index shows up in non-verbose listing");
        final List<SnapshotInfo> snapshotInfos = client().admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setVerbose(false)
            .get()
            .getSnapshots();
        assertThat(snapshotInfos, hasSize(1));
        final SnapshotInfo found = snapshotInfos.get(0);
        assertThat(found.snapshotId(), is(snapshotInfo.snapshotId()));
        assertThat(found.state(), is(SnapshotState.SUCCESS));
    }

    /**
     * Tests the following sequence of steps:
     * 1. Start snapshot of two shards (both located on separate data nodes).
     * 2. Have one of the shards snapshot completely and the other block
     * 3. Restart the data node that completed its shard snapshot
     * 4. Make sure that snapshot status APIs show correct file-counts and -sizes
     *
     * @throws Exception on failure
     */
    public void testCorrectCountsForDoneShards() throws Exception {
        final String indexOne = "index-1";
        final String indexTwo = "index-2";
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String dataNodeOne = dataNodes.get(0);
        final String dataNodeTwo = dataNodes.get(1);

        createIndex(indexOne, singleShardOneNode(dataNodeOne));
        index(indexOne, "_doc", "some_doc_id", "foo", "bar");
        createIndex(indexTwo, singleShardOneNode(dataNodeTwo));
        index(indexTwo, "_doc", "some_doc_id", "foo", "bar");

        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        blockDataNode(repoName, dataNodeOne);

        final String snapshotOne = "snap-1";
        // restarting a data node below so using a cluster-manager client here
        final ActionFuture<CreateSnapshotResponse> responseSnapshotOne = internalCluster().clusterManagerClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotOne)
            .setWaitForCompletion(true)
            .execute();

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            assertThat(snapshotStatusOne.getState(), is(SnapshotsInProgress.State.STARTED));
            final SnapshotIndexShardStatus snapshotShardState = stateFirstShard(snapshotStatusOne, indexTwo);
            assertThat(snapshotShardState.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardState.getStats().getTotalFileCount(), greaterThan(0));
            assertThat(snapshotShardState.getStats().getTotalSize(), greaterThan(0L));
        }, 30L, TimeUnit.SECONDS);

        final SnapshotStats snapshotShardStats = stateFirstShard(getSnapshotStatus(repoName, snapshotOne), indexTwo).getStats();
        final int totalFiles = snapshotShardStats.getTotalFileCount();
        final long totalFileSize = snapshotShardStats.getTotalSize();

        internalCluster().restartNode(dataNodeTwo);

        final SnapshotIndexShardStatus snapshotShardStateAfterNodeRestart = stateFirstShard(
            getSnapshotStatus(repoName, snapshotOne),
            indexTwo
        );
        assertThat(snapshotShardStateAfterNodeRestart.getStage(), is(SnapshotIndexShardStage.DONE));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalFileCount(), equalTo(totalFiles));
        assertThat(snapshotShardStateAfterNodeRestart.getStats().getTotalSize(), equalTo(totalFileSize));

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotOne.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));

        // indexing another document to the second index so it will do writes during the snapshot and we can block on those writes
        index(indexTwo, "_doc", "some_other_doc_id", "foo", "other_bar");

        blockDataNode(repoName, dataNodeTwo);

        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> responseSnapshotTwo = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotTwo)
            .setWaitForCompletion(true)
            .execute();

        waitForBlock(dataNodeTwo, repoName, TimeValue.timeValueSeconds(30L));

        assertBusy(() -> {
            final SnapshotStatus snapshotStatusOne = getSnapshotStatus(repoName, snapshotOne);
            final SnapshotStatus snapshotStatusTwo = getSnapshotStatus(repoName, snapshotTwo);
            final SnapshotIndexShardStatus snapshotShardStateOne = stateFirstShard(snapshotStatusOne, indexOne);
            final SnapshotIndexShardStatus snapshotShardStateTwo = stateFirstShard(snapshotStatusTwo, indexOne);
            assertThat(snapshotShardStateOne.getStage(), is(SnapshotIndexShardStage.DONE));
            assertThat(snapshotShardStateTwo.getStage(), is(SnapshotIndexShardStage.DONE));
            final int totalFilesShardOne = snapshotShardStateOne.getStats().getTotalFileCount();
            final long totalSizeShardOne = snapshotShardStateOne.getStats().getTotalSize();
            assertThat(totalFilesShardOne, greaterThan(0));
            assertThat(totalSizeShardOne, greaterThan(0L));
            assertThat(totalFilesShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalFileCount()));
            assertThat(totalSizeShardOne, equalTo(snapshotShardStateTwo.getStats().getTotalSize()));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalFileCount(), equalTo(0));
            assertThat(snapshotShardStateTwo.getStats().getIncrementalSize(), equalTo(0L));
        }, 30L, TimeUnit.SECONDS);

        unblockAllDataNodes(repoName);
        assertThat(responseSnapshotTwo.get().getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
    }

    public void testSnapshotStatusOnFailedSnapshot() throws Exception {
        String repoName = "test-repo";
        createRepository(repoName, "fs");
        final String snapshot = "test-snap-1";
        addBwCFailedSnapshot(repoName, snapshot, Collections.emptyMap());

        logger.info("--> creating good index");
        assertAcked(prepareCreate("test-idx-good").setSettings(indexSettingsNoReplicas(1)));
        ensureGreen();
        indexRandomDocs("test-idx-good", randomIntBetween(1, 5));

        final SnapshotsStatusResponse snapshotsStatusResponse = client().admin()
            .cluster()
            .prepareSnapshotStatus(repoName)
            .setSnapshots(snapshot)
            .get();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        assertEquals(SnapshotsInProgress.State.FAILED, snapshotsStatusResponse.getSnapshots().get(0).getState());
    }

    public void testSnapshotStatusOnPartialSnapshot() throws Exception {
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String snapshotName = "test-snap";
        final String indexName = "test-idx";
        createRepository(repoName, "fs");
        // create an index with a single shard on the data node, that will be stopped
        createIndex(indexName, singleShardOneNode(dataNode));
        index(indexName, "_doc", "some_doc_id", "foo", "bar");
        logger.info("--> stopping data node before creating snapshot");
        stopNode(dataNode);
        startFullSnapshot(repoName, snapshotName, true).get();
        final SnapshotStatus snapshotStatus = getSnapshotStatus(repoName, snapshotName);
        assertEquals(SnapshotsInProgress.State.PARTIAL, snapshotStatus.getState());
    }

    public void testStatusAPICallInProgressShallowSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        final String snapshotRepoName = "snapshot-repo-name";
        createRepository(snapshotRepoName, "mock", snapshotRepoSettingsForShallowCopy().put("block_on_data", true));

        final String indexName = "index-1";
        createIndex(indexName);
        ensureGreen();
        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        logger.info("--> snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot(snapshotRepoName, "test-snap");

        logger.info("--> wait for data nodes to get blocked");
        waitForBlockOnAnyDataNode(snapshotRepoName, TimeValue.timeValueMinutes(1));
        awaitNumberOfSnapshotsInProgress(1);
        assertEquals(
            SnapshotsInProgress.State.STARTED,
            client().admin()
                .cluster()
                .prepareSnapshotStatus(snapshotRepoName)
                .setSnapshots("test-snap")
                .get()
                .getSnapshots()
                .get(0)
                .getState()
        );

        logger.info("--> unblock all data nodes");
        unblockAllDataNodes(snapshotRepoName);

        logger.info("--> wait for snapshot to finish");
        createSnapshotResponseActionFuture.actionGet();
    }

    public void testGetSnapshotsRequest() throws Exception {
        final String repositoryName = "test-repo";
        final String indexName = "test-idx";
        final Client client = client();

        createRepository(
            repositoryName,
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put("wait_after_unblock", 200)
        );

        logger.info("--> get snapshots on an empty repository");
        expectThrows(
            SnapshotMissingException.class,
            () -> client.admin().cluster().prepareGetSnapshots(repositoryName).addSnapshots("non-existent-snapshot").get()
        );
        // with ignore unavailable set to true, should not throw an exception
        GetSnapshotsResponse getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .setIgnoreUnavailable(true)
            .addSnapshots("non-existent-snapshot")
            .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));

        logger.info("--> creating an index and indexing documents");
        // Create index on 2 nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(indexName, 1, Settings.builder().put("number_of_replicas", 0)));
        ensureGreen();
        indexRandomDocs(indexName, 10);

        // make sure we return only the in-progress snapshot when taking the first snapshot on a clean repository
        // take initial snapshot with a block, making sure we only get 1 in-progress snapshot returned
        // block a node so the create snapshot operation can remain in progress
        final String initialBlockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, "snap-on-empty-repo")
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .get();
        waitForBlock(initialBlockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setSnapshots(randomFrom("_all", "_current", "snap-on-*", "*-on-empty-repo", "snap-on-empty-repo"))
            .get();
        assertEquals(1, getSnapshotsResponse.getSnapshots().size());
        assertEquals("snap-on-empty-repo", getSnapshotsResponse.getSnapshots().get(0).snapshotId().getName());

        // there is an in-progress snapshot, make sure we return empty result when getting a non-existing snapshot with setting
        // ignore_unavailable to true
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots("test-repo")
            .setIgnoreUnavailable(true)
            .addSnapshots("non-existent-snapshot")
            .get();
        assertEquals(0, getSnapshotsResponse.getSnapshots().size());

        unblockNode(repositoryName, initialBlockedNode); // unblock node
        admin().cluster().prepareDeleteSnapshot(repositoryName, "snap-on-empty-repo").get();

        final int numSnapshots = randomIntBetween(1, 3) + 1;
        logger.info("--> take {} snapshot(s)", numSnapshots - 1);
        final String[] snapshotNames = new String[numSnapshots];
        for (int i = 0; i < numSnapshots - 1; i++) {
            final String snapshotName = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
            CreateSnapshotResponse createSnapshotResponse = client.admin()
                .cluster()
                .prepareCreateSnapshot(repositoryName, snapshotName)
                .setWaitForCompletion(true)
                .setIndices(indexName)
                .get();
            assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
            snapshotNames[i] = snapshotName;
        }
        logger.info("--> take another snapshot to be in-progress");
        // add documents so there are data files to block on
        for (int i = 10; i < 20; i++) {
            index(indexName, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();

        final String inProgressSnapshot = randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        snapshotNames[numSnapshots - 1] = inProgressSnapshot;
        // block a node so the create snapshot operation can remain in progress
        final String blockedNode = blockNodeWithIndex(repositoryName, indexName);
        client.admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, inProgressSnapshot)
            .setWaitForCompletion(false)
            .setIndices(indexName)
            .get();
        waitForBlock(blockedNode, repositoryName, TimeValue.timeValueSeconds(60)); // wait for block to kick in

        logger.info("--> get all snapshots with a current in-progress");
        // with ignore unavailable set to true, should not throw an exception
        final List<String> snapshotsToGet = new ArrayList<>();
        if (randomBoolean()) {
            // use _current plus the individual names of the finished snapshots
            snapshotsToGet.add("_current");
            for (int i = 0; i < numSnapshots - 1; i++) {
                snapshotsToGet.add(snapshotNames[i]);
            }
        } else {
            snapshotsToGet.add("_all");
        }
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .setSnapshots(snapshotsToGet.toArray(Strings.EMPTY_ARRAY))
            .get();
        List<String> sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(
            getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().collect(Collectors.toList()),
            equalTo(sortedNames)
        );

        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots(repositoryName).addSnapshots(snapshotNames).get();
        sortedNames = Arrays.asList(snapshotNames);
        Collections.sort(sortedNames);
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(
            getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().collect(Collectors.toList()),
            equalTo(sortedNames)
        );

        logger.info("--> make sure duplicates are not returned in the response");
        String regexName = snapshotNames[randomIntBetween(0, numSnapshots - 1)];
        final int splitPos = regexName.length() / 2;
        final String firstRegex = regexName.substring(0, splitPos) + "*";
        final String secondRegex = "*" + regexName.substring(splitPos);
        getSnapshotsResponse = client.admin()
            .cluster()
            .prepareGetSnapshots(repositoryName)
            .addSnapshots(snapshotNames)
            .addSnapshots(firstRegex, secondRegex)
            .get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(numSnapshots));
        assertThat(
            getSnapshotsResponse.getSnapshots().stream().map(s -> s.snapshotId().getName()).sorted().collect(Collectors.toList()),
            equalTo(sortedNames)
        );

        unblockNode(repositoryName, blockedNode); // unblock node
        waitForCompletion(repositoryName, inProgressSnapshot, TimeValue.timeValueSeconds(60));
    }

    public void testSnapshotStatusApiFailureForTooManyShardsAcrossSnapshots() throws Exception {
        String repositoryName = "test-repo";
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        String index3 = "test-idx-3";
        createRepository(repositoryName, "fs");

        logger.info("Create indices");
        createIndex(index1, index2, index3);
        ensureGreen();

        logger.info("Indexing some data");
        for (int i = 0; i < 10; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "baz" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        String snapshot1 = "test-snap-1";
        String snapshot2 = "test-snap-2";
        createSnapshot(repositoryName, snapshot1, List.of(index1, index2, index3));
        createSnapshot(repositoryName, snapshot2, List.of(index1, index2));

        logger.info("Set MAX_SHARDS_ALLOWED_IN_STATUS_API to a low value");
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey(), 2));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // across a single snapshot
        assertBusy(() -> {
            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin().cluster().prepareSnapshotStatus(repositoryName).setSnapshots(snapshot1).execute().actionGet()
            );
            assertEquals(exception.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(
                exception.getMessage().contains(" is more than the maximum allowed value of shard count [2] for snapshot status request")
            );
        });

        // across multiple snapshots
        assertBusy(() -> {
            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(snapshot1, snapshot2)
                    .execute()
                    .actionGet()
            );
            assertEquals(exception.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(
                exception.getMessage().contains(" is more than the maximum allowed value of shard count [2] for snapshot status request")
            );
        });

        logger.info("Reset MAX_SHARDS_ALLOWED_IN_STATUS_API to default value");
        updateSettingsRequest.persistentSettings(Settings.builder().putNull(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey()));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    public void testSnapshotStatusForIndexFilter() throws Exception {
        String repositoryName = "test-repo";
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        String index3 = "test-idx-3";
        createRepository(repositoryName, "fs");

        logger.info("Create indices");
        createIndex(index1, index2, index3);
        ensureGreen();

        logger.info("Indexing some data");
        for (int i = 0; i < 10; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "baz" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        String snapshot = "test-snap-1";
        createSnapshot(repositoryName, snapshot, List.of(index1, index2, index3));

        // for a completed snapshot
        assertBusy(() -> {
            SnapshotStatus snapshotsStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(repositoryName)
                .setSnapshots(snapshot)
                .setIndices(index1, index2)
                .get()
                .getSnapshots()
                .get(0);
            Map<String, SnapshotIndexStatus> snapshotIndexStatusMap = snapshotsStatus.getIndices();
            // Although the snapshot contains 3 indices, the response of status api call only contains results for 2
            assertEquals(snapshotIndexStatusMap.size(), 2);
            assertEquals(snapshotIndexStatusMap.keySet(), Set.of(index1, index2));
        }, 1, TimeUnit.MINUTES);
    }

    public void testSnapshotStatusForIndexFilterForInProgressSnapshot() throws Exception {
        String repositoryName = "test-repo";
        createRepository(repositoryName, "mock", Settings.builder().put("location", randomRepoPath()).put("block_on_data", true));

        logger.info("Create indices");
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        String index3 = "test-idx-3";
        createIndex(index1, index2, index3);
        ensureGreen();

        logger.info("Indexing some data");
        for (int i = 0; i < 10; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "baz" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        String inProgressSnapshot = "test-in-progress-snapshot";

        logger.info("Create snapshot");
        ActionFuture<CreateSnapshotResponse> createSnapshotResponseActionFuture = startFullSnapshot(repositoryName, inProgressSnapshot);

        logger.info("Block data node");
        waitForBlockOnAnyDataNode(repositoryName, TimeValue.timeValueMinutes(1));
        awaitNumberOfSnapshotsInProgress(1);

        // test normal functioning of index filter for in progress snapshot
        assertBusy(() -> {
            SnapshotStatus snapshotsStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(repositoryName)
                .setSnapshots(inProgressSnapshot)
                .setIndices(index1, index2)
                .get()
                .getSnapshots()
                .get(0);
            Map<String, SnapshotIndexStatus> snapshotIndexStatusMap = snapshotsStatus.getIndices();
            // Although the snapshot contains 3 indices, the response of status api call only contains results for 2
            assertEquals(snapshotIndexStatusMap.size(), 2);
            assertEquals(snapshotIndexStatusMap.keySet(), Set.of(index1, index2));
        });

        // when a non-existent index is requested in the index-filter
        assertBusy(() -> {
            // failure due to index not found in snapshot
            final String nonExistentIndex1 = "non-existent-index-1";
            final String nonExistentIndex2 = "non-existent-index-2";
            Exception ex = expectThrows(
                Exception.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(inProgressSnapshot)
                    .setIndices(index1, index2, nonExistentIndex1, nonExistentIndex2)
                    .execute()
                    .actionGet()
            );
            String cause = String.format(
                Locale.ROOT,
                "indices [%s] missing in snapshot [%s] of repository [%s]",
                String.join(", ", List.of(nonExistentIndex2, nonExistentIndex1)),
                inProgressSnapshot,
                repositoryName
            );
            assertEquals(cause, ex.getCause().getMessage());

            // no error for ignore_unavailable = true and status response contains only the found indices
            SnapshotStatus snapshotsStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(repositoryName)
                .setSnapshots(inProgressSnapshot)
                .setIndices(index1, index2, nonExistentIndex1, nonExistentIndex2)
                .setIgnoreUnavailable(true)
                .get()
                .getSnapshots()
                .get(0);

            Map<String, SnapshotIndexStatus> snapshotIndexStatusMap = snapshotsStatus.getIndices();
            assertEquals(snapshotIndexStatusMap.size(), 2);
            assertEquals(snapshotIndexStatusMap.keySet(), Set.of(index1, index2));
        });

        logger.info("Unblock data node");
        unblockAllDataNodes(repositoryName);

        logger.info("Wait for snapshot to finish");
        waitForCompletion(repositoryName, inProgressSnapshot, TimeValue.timeValueSeconds(60));
    }

    public void testSnapshotStatusFailuresWithIndexFilter() throws Exception {
        String repositoryName = "test-repo";
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        String index3 = "test-idx-3";
        createRepository(repositoryName, "fs");

        logger.info("Create indices");
        createIndex(index1, index2, index3);
        ensureGreen();

        logger.info("Indexing some data");
        for (int i = 0; i < 10; i++) {
            index(index1, "_doc", Integer.toString(i), "foo", "bar" + i);
            index(index2, "_doc", Integer.toString(i), "foo", "baz" + i);
            index(index3, "_doc", Integer.toString(i), "foo", "baz" + i);
        }
        refresh();
        String snapshot1 = "test-snap-1";
        String snapshot2 = "test-snap-2";
        createSnapshot(repositoryName, snapshot1, List.of(index1, index2, index3));
        createSnapshot(repositoryName, snapshot2, List.of(index1));

        assertBusy(() -> {
            // failure due to passing index filter for _all value of repository param
            Exception ex = expectThrows(
                Exception.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus("_all")
                    .setSnapshots(snapshot1)
                    .setIndices(index1, index2, index3)
                    .execute()
                    .actionGet()
            );
            String cause =
                "index list filter is supported only when a single 'repository' is passed, but found 'repository' param = [_all]";
            assertTrue(ex.getMessage().contains(cause));
        });

        assertBusy(() -> {
            // failure due to passing index filter for _all value of snapshot param --> gets translated as a blank array
            Exception ex = expectThrows(
                Exception.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots()
                    .setIndices(index1, index2, index3)
                    .execute()
                    .actionGet()
            );
            String cause = "index list filter is supported only when a single 'snapshot' is passed, but found 'snapshot' param = [_all]";
            assertTrue(ex.getMessage().contains(cause));
        });

        assertBusy(() -> {
            // failure due to passing index filter for multiple snapshots
            ActionRequestValidationException ex = expectThrows(
                ActionRequestValidationException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(snapshot1, snapshot2)
                    .setIndices(index1, index2, index3)
                    .execute()
                    .actionGet()
            );
            String cause =
                "index list filter is supported only when a single 'snapshot' is passed, but found 'snapshot' param = [[test-snap-1, test-snap-2]]";
            assertTrue(ex.getMessage().contains(cause));
        });

        assertBusy(() -> {
            // failure due to index not found in snapshot
            IndexNotFoundException ex = expectThrows(
                IndexNotFoundException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(snapshot2)
                    .setIndices(index1, index2, index3)
                    .execute()
                    .actionGet()
            );
            assertEquals(ex.status(), RestStatus.NOT_FOUND);
            String cause = String.format(
                Locale.ROOT,
                "indices [%s] missing in snapshot [%s] of repository [%s]",
                String.join(", ", List.of(index2, index3)),
                snapshot2,
                repositoryName
            );
            assertEquals(cause, ex.getCause().getMessage());

            // no error for ignore_unavailable = true and status response contains only the found indices
            SnapshotStatus snapshotsStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(repositoryName)
                .setSnapshots(snapshot2)
                .setIndices(index1, index2, index3)
                .setIgnoreUnavailable(true)
                .get()
                .getSnapshots()
                .get(0);
            assertEquals(1, snapshotsStatus.getIndices().size());
        });

        assertBusy(() -> {
            // failure due to too many shards requested
            logger.info("Set MAX_SHARDS_ALLOWED_IN_STATUS_API to a low value");
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey(), 2));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            CircuitBreakingException ex = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(snapshot1)
                    .setIndices(index1, index2, index3)
                    .execute()
                    .actionGet()
            );
            assertEquals(ex.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(ex.getMessage().contains(" is more than the maximum allowed value of shard count [2] for snapshot status request"));

            logger.info("Reset MAX_SHARDS_ALLOWED_IN_STATUS_API to default value");
            updateSettingsRequest.persistentSettings(Settings.builder().putNull(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey()));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        });
    }

    public void testSnapshotStatusShardLimitOfResponseForInProgressSnapshot() throws Exception {
        logger.info("Create repository");
        String repositoryName = "test-repo";
        createRepository(
            repositoryName,
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", false)
                .put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                .put("wait_after_unblock", 200)
        );

        logger.info("Create indices");
        String index1 = "test-idx-1";
        String index2 = "test-idx-2";
        String index3 = "test-idx-3";
        assertAcked(prepareCreate(index1, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(index2, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        assertAcked(prepareCreate(index3, 1, Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("Index some data");
        indexRandomDocs(index1, 10);
        indexRandomDocs(index2, 10);
        indexRandomDocs(index3, 10);

        logger.info("Create completed snapshot");
        String completedSnapshot = "test-completed-snapshot";
        String blockedNode = blockNodeWithIndex(repositoryName, index1);
        client().admin().cluster().prepareCreateSnapshot(repositoryName, completedSnapshot).setWaitForCompletion(false).get();
        waitForBlock(blockedNode, repositoryName, TimeValue.timeValueSeconds(60));
        unblockNode(repositoryName, blockedNode);
        waitForCompletion(repositoryName, completedSnapshot, TimeValue.timeValueSeconds(60));

        logger.info("Index some more data");
        indexRandomDocs(index1, 10);
        indexRandomDocs(index2, 10);
        indexRandomDocs(index3, 10);
        refresh();

        logger.info("Create in-progress snapshot");
        String inProgressSnapshot = "test-in-progress-snapshot";
        blockedNode = blockNodeWithIndex(repositoryName, index1);
        client().admin().cluster().prepareCreateSnapshot(repositoryName, inProgressSnapshot).setWaitForCompletion(false).get();
        waitForBlock(blockedNode, repositoryName, TimeValue.timeValueSeconds(60));
        List<SnapshotStatus> snapshotStatuses = client().admin()
            .cluster()
            .prepareSnapshotStatus(repositoryName)
            .setSnapshots(inProgressSnapshot, completedSnapshot)
            .get()
            .getSnapshots();

        assertEquals(2, snapshotStatuses.size());
        assertEquals(SnapshotsInProgress.State.STARTED, snapshotStatuses.get(0).getState());
        assertEquals(SnapshotsInProgress.State.SUCCESS, snapshotStatuses.get(1).getState());

        logger.info("Set MAX_SHARDS_ALLOWED_IN_STATUS_API to a low value");
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(Settings.builder().put(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey(), 1));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // shard limit exceeded due to inProgress snapshot alone @ without index-filter
        assertBusy(() -> {
            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(inProgressSnapshot)
                    .execute()
                    .actionGet()
            );
            assertEquals(exception.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(
                exception.getMessage().contains(" is more than the maximum allowed value of shard count [1] for snapshot status request")
            );
        });

        // shard limit exceeded due to inProgress snapshot alone @ with index-filter
        assertBusy(() -> {
            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(inProgressSnapshot)
                    .setIndices(index1, index2)
                    .execute()
                    .actionGet()
            );
            assertEquals(exception.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(
                exception.getMessage().contains(" is more than the maximum allowed value of shard count [1] for snapshot status request")
            );
        });

        logger.info("Set MAX_SHARDS_ALLOWED_IN_STATUS_API to a slightly higher value");
        updateSettingsRequest.persistentSettings(Settings.builder().put(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey(), 5));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

        // shard limit exceeded due to passing for inProgress but failing for current + completed

        assertBusy(() -> {
            SnapshotStatus inProgressSnapshotStatus = client().admin()
                .cluster()
                .prepareSnapshotStatus(repositoryName)
                .setSnapshots(inProgressSnapshot)
                .get()
                .getSnapshots()
                .get(0);
            assertEquals(3, inProgressSnapshotStatus.getShards().size());

            CircuitBreakingException exception = expectThrows(
                CircuitBreakingException.class,
                () -> client().admin()
                    .cluster()
                    .prepareSnapshotStatus(repositoryName)
                    .setSnapshots(inProgressSnapshot, completedSnapshot)
                    .execute()
                    .actionGet()
            );
            assertEquals(exception.status(), RestStatus.TOO_MANY_REQUESTS);
            assertTrue(
                exception.getMessage().contains(" is more than the maximum allowed value of shard count [5] for snapshot status request")
            );
        });

        unblockNode(repositoryName, blockedNode);
        waitForCompletion(repositoryName, inProgressSnapshot, TimeValue.timeValueSeconds(60));

        logger.info("Reset MAX_SHARDS_ALLOWED_IN_STATUS_API to default value");
        updateSettingsRequest.persistentSettings(Settings.builder().putNull(MAX_SHARDS_ALLOWED_IN_STATUS_API.getKey()));
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    private static SnapshotIndexShardStatus stateFirstShard(SnapshotStatus snapshotStatus, String indexName) {
        return snapshotStatus.getIndices().get(indexName).getShards().get(0);
    }

    private static SnapshotStatus getSnapshotStatus(String repoName, String snapshotName) {
        try {
            return client().admin().cluster().prepareSnapshotStatus(repoName).setSnapshots(snapshotName).get().getSnapshots().get(0);
        } catch (SnapshotMissingException e) {
            throw new AssertionError(e);
        }
    }

    private static Settings singleShardOneNode(String node) {
        return indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", node).build();
    }
}
