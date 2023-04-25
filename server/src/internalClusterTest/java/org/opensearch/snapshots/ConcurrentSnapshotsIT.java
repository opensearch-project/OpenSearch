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

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.SnapshotDeletionsInProgress;
import org.opensearch.cluster.SnapshotsInProgress;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.UncategorizedExecutionException;
import org.opensearch.discovery.AbstractDisruptionTestCase;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.RepositoryException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.snapshots.mockstore.MockRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFileExists;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ConcurrentSnapshotsIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, MockRepository.Plugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put(AbstractDisruptionTestCase.DEFAULT_SETTINGS).build();
    }

    public void testLongRunningSnapshotAllowsConcurrentSnapshot() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "slow-snapshot",
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String indexFast = "index-fast";
        createIndexWithContent(indexFast, dataNode2, dataNode);

        assertSuccessful(
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, "fast-snapshot")
                .setIndices(indexFast)
                .setWaitForCompletion(true)
                .execute()
        );

        assertThat(createSlowFuture.isDone(), is(false));
        unblockNode(repoName, dataNode);

        assertSuccessful(createSlowFuture);
    }

    public void testDeletesAreBatched() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        createIndex("foo");
        ensureGreen();

        final int numSnapshots = randomIntBetween(1, 4);
        final PlainActionFuture<Collection<CreateSnapshotResponse>> allSnapshotsDone = PlainActionFuture.newFuture();
        final ActionListener<CreateSnapshotResponse> snapshotsListener = new GroupedActionListener<>(allSnapshotsDone, numSnapshots);
        final Collection<String> snapshotNames = new HashSet<>();
        for (int i = 0; i < numSnapshots; i++) {
            final String snapshot = "snap-" + i;
            snapshotNames.add(snapshot);
            client().admin().cluster().prepareCreateSnapshot(repoName, snapshot).setWaitForCompletion(true).execute(snapshotsListener);
        }
        final Collection<CreateSnapshotResponse> snapshotResponses = allSnapshotsDone.get();
        for (CreateSnapshotResponse snapshotResponse : snapshotResponses) {
            assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        }

        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            repoName,
            dataNode
        );

        final Collection<StepListener<AcknowledgedResponse>> deleteFutures = new ArrayList<>();
        while (snapshotNames.isEmpty() == false) {
            final Collection<String> toDelete = randomSubsetOf(snapshotNames);
            if (toDelete.isEmpty()) {
                continue;
            }
            snapshotNames.removeAll(toDelete);
            final StepListener<AcknowledgedResponse> future = new StepListener<>();
            client().admin().cluster().prepareDeleteSnapshot(repoName, toDelete.toArray(Strings.EMPTY_ARRAY)).execute(future);
            deleteFutures.add(future);
        }

        assertThat(createSlowFuture.isDone(), is(false));

        final long repoGenAfterInitialSnapshots = getRepositoryData(repoName).getGenId();
        assertThat(repoGenAfterInitialSnapshots, is(numSnapshots - 1L));
        unblockNode(repoName, dataNode);

        final SnapshotInfo slowSnapshotInfo = assertSuccessful(createSlowFuture);

        logger.info("--> waiting for batched deletes to finish");
        final PlainActionFuture<Collection<AcknowledgedResponse>> allDeletesDone = new PlainActionFuture<>();
        final ActionListener<AcknowledgedResponse> deletesListener = new GroupedActionListener<>(allDeletesDone, deleteFutures.size());
        for (StepListener<AcknowledgedResponse> deleteFuture : deleteFutures) {
            deleteFuture.whenComplete(deletesListener::onResponse, deletesListener::onFailure);
        }
        allDeletesDone.get();

        logger.info("--> verifying repository state");
        final RepositoryData repositoryDataAfterDeletes = getRepositoryData(repoName);
        // One increment for snapshot, one for all the deletes
        assertThat(repositoryDataAfterDeletes.getGenId(), is(repoGenAfterInitialSnapshots + 2));
        assertThat(repositoryDataAfterDeletes.getSnapshotIds(), contains(slowSnapshotInfo.snapshotId()));
    }

    public void testBlockedRepoDoesNotBlockOtherRepos() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndex("foo");
        ensureGreen();
        createIndexWithContent("index-slow");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startAndBlockFailingFullSnapshot(blockedRepoName, "blocked-snapshot");

        client().admin()
            .cluster()
            .prepareCreateSnapshot(otherRepoName, "snapshot")
            .setIndices("does-not-exist-*")
            .setWaitForCompletion(false)
            .get();

        unblockNode(blockedRepoName, internalCluster().getClusterManagerName());
        expectThrows(SnapshotException.class, createSlowFuture::actionGet);

        assertBusy(() -> assertThat(currentSnapshots(otherRepoName), empty()), 30L, TimeUnit.SECONDS);
    }

    public void testMultipleReposAreIndependent() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testMultipleReposAreIndependent2() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second repository's concurrent operations.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        assertAcked(startDeleteSnapshot(otherRepoName, "*").get());

        unblockNode(blockedRepoName, dataNode);
        assertSuccessful(createSlowFuture);
    }

    public void testMultipleReposAreIndependent3() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String blockedRepoName = "test-repo-blocked";
        final String otherRepoName = "test-repo";
        createRepository(blockedRepoName, "mock");
        createRepository(otherRepoName, "fs");
        createIndexWithContent("test-index");

        createFullSnapshot(blockedRepoName, "blocked-snapshot");
        blockNodeOnAnyFiles(blockedRepoName, clusterManagerNode);
        final ActionFuture<AcknowledgedResponse> slowDeleteFuture = startDeleteSnapshot(blockedRepoName, "*");

        logger.info("--> waiting for concurrent snapshot(s) to finish");
        createNSnapshots(otherRepoName, randomIntBetween(1, 5));
        assertAcked(startDeleteSnapshot(otherRepoName, "*").get());

        unblockNode(blockedRepoName, clusterManagerNode);
        assertAcked(slowDeleteFuture.actionGet());
    }

    public void testSnapshotRunsAfterInProgressDelete() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        ensureGreen();
        createIndexWithContent("index-test");

        final String firstSnapshot = "first-snapshot";
        createFullSnapshot(repoName, firstSnapshot);

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, firstSnapshot);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "second-snapshot");

        unblockNode(repoName, clusterManagerNode);
        final UncategorizedExecutionException ex = expectThrows(UncategorizedExecutionException.class, deleteFuture::actionGet);
        assertThat(ex.getRootCause(), instanceOf(IOException.class));

        assertSuccessful(snapshotFuture);
    }

    public void testAbortOneOfMultipleSnapshots() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotBlockedOnDataNode(
            firstSnapshot,
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        logger.info("--> start third snapshot");
        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-three")
            .setIndices(secondIndex)
            .setWaitForCompletion(true)
            .execute();

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        unblockNode(repoName, dataNode);
        final SnapshotInfo firstSnapshotInfo = firstSnapshotResponse.get().getSnapshotInfo();
        assertThat(firstSnapshotInfo.state(), is(SnapshotState.FAILED));
        assertThat(firstSnapshotInfo.reason(), is("Snapshot was aborted by deletion"));

        final SnapshotInfo secondSnapshotInfo = assertSuccessful(secondSnapshotResponse);
        final SnapshotInfo thirdSnapshotInfo = assertSuccessful(thirdSnapshotResponse);

        assertThat(deleteSnapshotsResponse.get().isAcknowledged(), is(true));

        logger.info("--> verify that the first snapshot is gone");
        assertThat(
            client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(),
            containsInAnyOrder(secondSnapshotInfo, thirdSnapshotInfo)
        );
    }

    public void testCascadedAborts() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");

        final String firstSnapshot = "snapshot-one";
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotBlockedOnDataNode(
            firstSnapshot,
            repoName,
            dataNode
        );

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        createIndexWithContent("index-two", dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> deleteSnapshotsResponse = startDeleteSnapshot(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        final ActionFuture<CreateSnapshotResponse> thirdSnapshotResponse = startFullSnapshot(repoName, "snapshot-three");

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> allDeletedResponse = startDeleteSnapshot(repoName, "*");

        logger.info("--> waiting for second and third snapshot to finish");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(1));
            final SnapshotsInProgress snapshotsInProgress = clusterService().state().custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries().get(0).state(), is(SnapshotsInProgress.State.ABORTED));
        }, 30L, TimeUnit.SECONDS);

        unblockNode(repoName, dataNode);

        logger.info("--> verify all snapshots were aborted");
        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));
        assertThat(thirdSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.FAILED));

        logger.info("--> verify both deletes have completed");
        assertAcked(deleteSnapshotsResponse.get());
        assertAcked(allDeletedResponse.get());

        logger.info("--> verify that all snapshots are gone");
        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testClusterManagerFailOverWithQueuedDeletes() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String firstIndex = "index-one";
        createIndexWithContent(firstIndex);

        final String firstSnapshot = "snapshot-one";
        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromNonClusterManagerClient(
            repoName,
            firstSnapshot
        );
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        final String dataNode2 = internalCluster().startDataOnlyNode();
        ensureStableCluster(5);
        final String secondIndex = "index-two";
        createIndexWithContent(secondIndex, dataNode2, dataNode);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshot(repoName, secondSnapshot);

        logger.info("--> wait for snapshot on second data node to finish");
        awaitClusterState(state -> {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            return snapshotsInProgress.entries().size() == 2 && snapshotHasCompletedShard(secondSnapshot, snapshotsInProgress);
        });

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDeleteFromNonClusterManagerClient(repoName, firstSnapshot);
        awaitNDeletionsInProgress(1);

        blockNodeOnAnyFiles(repoName, dataNode2);
        final ActionFuture<CreateSnapshotResponse> snapshotThreeFuture = startFullSnapshotFromNonClusterManagerClient(
            repoName,
            "snapshot-three"
        );
        waitForBlock(dataNode2, repoName, TimeValue.timeValueSeconds(30L));

        assertThat(firstSnapshotResponse.isDone(), is(false));
        assertThat(secondSnapshotResponse.isDone(), is(false));

        logger.info("--> waiting for all three snapshots to show up as in-progress");
        assertBusy(() -> assertThat(currentSnapshots(repoName), hasSize(3)), 30L, TimeUnit.SECONDS);

        final ActionFuture<AcknowledgedResponse> deleteAllSnapshots = startDeleteFromNonClusterManagerClient(repoName, "*");
        logger.info("--> wait for delete to be enqueued in cluster state");
        awaitClusterState(state -> {
            final SnapshotDeletionsInProgress deletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            return deletionsInProgress.getEntries().size() == 1 && deletionsInProgress.getEntries().get(0).getSnapshots().size() == 3;
        });

        logger.info("--> waiting for second snapshot to finish and the other two snapshots to become aborted");
        assertBusy(() -> {
            assertThat(currentSnapshots(repoName), hasSize(2));
            for (SnapshotsInProgress.Entry entry : clusterService().state()
                .custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY)
                .entries()) {
                assertThat(entry.state(), is(SnapshotsInProgress.State.ABORTED));
                assertThat(entry.snapshot().getSnapshotId().getName(), not(secondSnapshot));
            }
        }, 30L, TimeUnit.SECONDS);

        logger.info("--> stopping current cluster-manager node");
        internalCluster().stopCurrentClusterManagerNode();

        unblockNode(repoName, dataNode);
        unblockNode(repoName, dataNode2);

        for (ActionFuture<AcknowledgedResponse> deleteFuture : Arrays.asList(firstDeleteFuture, deleteAllSnapshots)) {
            try {
                assertAcked(deleteFuture.actionGet());
            } catch (RepositoryException rex) {
                // rarely the cluster-manager node fails over twice
                // when shutting down the initial cluster-manager and fails the transport listener
                assertThat(rex.repository(), is("_all"));
                assertThat(rex.getMessage(), endsWith("Failed to update cluster state during repository operation"));
            } catch (SnapshotMissingException sme) {
                // very rarely a cluster-manager node fail-over happens at such a time
                // that the client on the data-node sees a disconnect exception after the cluster-manager has already started the delete,
                // leading to the delete retry to run into a situation where the snapshot has already been deleted potentially
                assertThat(sme.getSnapshotName(), is(firstSnapshot));
            }
        }
        expectThrows(SnapshotException.class, snapshotThreeFuture::actionGet);

        logger.info("--> verify that all snapshots are gone and no more work is left in the cluster state");
        assertBusy(() -> {
            assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
            final ClusterState state = clusterService().state();
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            assertThat(snapshotsInProgress.entries(), empty());
            final SnapshotDeletionsInProgress snapshotDeletionsInProgress = state.custom(SnapshotDeletionsInProgress.TYPE);
            assertThat(snapshotDeletionsInProgress.getEntries(), empty());
        }, 30L, TimeUnit.SECONDS);
    }

    public void testAssertMultipleSnapshotsAndPrimaryFailOver() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        final String testIndex = "index-one";
        createIndex(testIndex, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(testIndex);
        index(testIndex, "_doc", "some_id", "foo", "bar");

        blockDataNode(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> firstSnapshotResponse = startFullSnapshotFromClusterManagerClient(
            repoName,
            "snapshot-one"
        );
        waitForBlock(dataNode, repoName, TimeValue.timeValueSeconds(30L));

        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        ensureGreen(testIndex);

        final String secondSnapshot = "snapshot-two";
        final ActionFuture<CreateSnapshotResponse> secondSnapshotResponse = startFullSnapshotFromClusterManagerClient(
            repoName,
            secondSnapshot
        );

        // make sure second snapshot is in progress before restarting data node
        waitUntilInprogress(repoName, secondSnapshot, TimeValue.timeValueSeconds(5L));

        internalCluster().restartNode(dataNode, InternalTestCluster.EMPTY_CALLBACK);

        assertThat(firstSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(secondSnapshotResponse.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
    }

    public void testQueuedDeletesWithFailures() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startDeleteSnapshot(repoName, "*");
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, clusterManagerNode);
        expectThrows(UncategorizedExecutionException.class, firstDeleteFuture::actionGet);

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        final SnapshotException snapshotException = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(snapshotException.getMessage(), containsString(SnapshotsInProgress.ABORTED_FAILURE_TEXT));

        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testQueuedDeletesWithOverlap() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<AcknowledgedResponse> firstDeleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, "snapshot-queued");
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, clusterManagerNode);
        assertThat(firstDeleteFuture.get().isAcknowledged(), is(true));

        // Second delete works out cleanly since the repo is unblocked now
        assertThat(secondDeleteFuture.get().isAcknowledged(), is(true));
        // Snapshot should have been aborted
        final SnapshotException snapshotException = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(snapshotException.getMessage(), containsString(SnapshotsInProgress.ABORTED_FAILURE_TEXT));

        assertThat(client().admin().cluster().prepareGetSnapshots(repoName).get().getSnapshots(), empty());
    }

    public void testQueuedOperationsOnClusterManagerRestart() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        startAndBlockOnDeleteSnapshot(repoName, "*");

        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        startDeleteSnapshot(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsOnClusterManagerDisconnect() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String clusterManagerNode = internalCluster().getClusterManagerName();
        final NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        blockNodeOnAnyFiles(repoName, clusterManagerNode);
        ActionFuture<AcknowledgedResponse> firstDeleteFuture = client(clusterManagerNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        final ActionFuture<CreateSnapshotResponse> createThirdSnapshot = client(clusterManagerNode).admin()
            .cluster()
            .prepareCreateSnapshot(repoName, "snapshot-three")
            .setWaitForCompletion(true)
            .execute();
        awaitNumberOfSnapshotsInProgress(1);

        final ActionFuture<AcknowledgedResponse> secondDeleteFuture = client(clusterManagerNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        awaitNDeletionsInProgress(2);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, clusterManagerNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        expectThrows(RepositoryException.class, firstDeleteFuture::actionGet);
        expectThrows(RepositoryException.class, secondDeleteFuture::actionGet);
        expectThrows(SnapshotException.class, createThirdSnapshot::actionGet);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsOnClusterManagerDisconnectAndRepoFailure() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String clusterManagerNode = internalCluster().getClusterManagerName();
        final NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> firstFailedSnapshotFuture = startFullSnapshotFromClusterManagerClient(
            repoName,
            "failing-snapshot-1"
        );
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        final ActionFuture<CreateSnapshotResponse> secondFailedSnapshotFuture = startFullSnapshotFromClusterManagerClient(
            repoName,
            "failing-snapshot-2"
        );
        awaitNumberOfSnapshotsInProgress(2);

        final ActionFuture<AcknowledgedResponse> failedDeleteFuture = client(clusterManagerNode).admin()
            .cluster()
            .prepareDeleteSnapshot(repoName, "*")
            .execute();
        awaitNDeletionsInProgress(1);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, clusterManagerNode);
        networkDisruption.stopDisrupting();

        logger.info("--> make sure all failing requests get a response");
        expectThrows(SnapshotException.class, firstFailedSnapshotFuture::actionGet);
        expectThrows(SnapshotException.class, secondFailedSnapshotFuture::actionGet);
        expectThrows(RepositoryException.class, failedDeleteFuture::actionGet);

        awaitNoMoreRunningOperations();
    }

    public void testQueuedOperationsAndBrokenRepoOnClusterManagerFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();

        startAndBlockOnDeleteSnapshot(repoName, "*");

        corruptIndexN(repoPath, generation);

        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-three").setWaitForCompletion(false).get();

        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteFromNonClusterManagerClient(repoName, "*");
        awaitNDeletionsInProgress(2);

        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(RepositoryException.class, deleteFuture::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnClusterManagerFailOver() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        blockNodeOnAnyFiles(repoName, clusterManagerNode);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-three");
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-four");
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(OpenSearchException.class, snapshotThree::actionGet);
        expectThrows(OpenSearchException.class, snapshotFour::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnClusterManagerFailOver2() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final long generation = getRepositoryData(repoName).getGenId();
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-three");
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-four");
        awaitNumberOfSnapshotsInProgress(2);

        final NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);
        unblockNode(repoName, clusterManagerNode);
        networkDisruption.stopDisrupting();
        awaitNoMoreRunningOperations();
        expectThrows(OpenSearchException.class, snapshotThree::actionGet);
        expectThrows(OpenSearchException.class, snapshotFour::actionGet);
    }

    public void testQueuedSnapshotOperationsAndBrokenRepoOnClusterManagerFailOverMultipleRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startClusterManagerOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));

        final String clusterManagerNode = internalCluster().getClusterManagerName();

        final String blockedRepoName = "repo-blocked";
        createRepository(blockedRepoName, "mock");
        createNSnapshots(blockedRepoName, randomIntBetween(1, 5));
        blockNodeOnAnyFiles(blockedRepoName, clusterManagerNode);
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteFromNonClusterManagerClient(blockedRepoName, "*");
        waitForBlock(clusterManagerNode, blockedRepoName, TimeValue.timeValueSeconds(30L));
        awaitNDeletionsInProgress(1);
        final ActionFuture<CreateSnapshotResponse> createBlockedSnapshot = startFullSnapshotFromNonClusterManagerClient(
            blockedRepoName,
            "queued-snapshot"
        );
        awaitNumberOfSnapshotsInProgress(1);

        final long generation = getRepositoryData(repoName).getGenId();
        blockNodeOnAnyFiles(repoName, clusterManagerNode);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-three");
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        awaitNumberOfSnapshotsInProgress(2);

        corruptIndexN(repoPath, generation);

        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshotFromNonClusterManagerClient(repoName, "snapshot-four");
        awaitNumberOfSnapshotsInProgress(3);
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3);

        awaitNoMoreRunningOperations();
        expectThrows(OpenSearchException.class, snapshotThree::actionGet);
        expectThrows(OpenSearchException.class, snapshotFour::actionGet);
        assertAcked(deleteFuture.get());
        try {
            createBlockedSnapshot.actionGet();
        } catch (OpenSearchException ex) {
            // Ignored, thrown most of the time but due to retries when shutting down the cluster-manager could randomly pass
            // when the request is retried and gets executed after the above delete
        }
    }

    public void testMultipleSnapshotsQueuedAfterDelete() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final ActionFuture<AcknowledgedResponse> deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshot(repoName, "snapshot-three");
        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshot(repoName, "snapshot-four");

        unblockNode(repoName, clusterManagerNode);

        assertSuccessful(snapshotThree);
        assertSuccessful(snapshotFour);
        assertAcked(deleteFuture.get());
    }

    public void testMultiplePartialSnapshotsQueuedAfterDelete() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-one");
        createIndexWithContent("index-two");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        final ActionFuture<AcknowledgedResponse> deleteFuture = startAndBlockOnDeleteSnapshot(repoName, "*");
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshot(repoName, "snapshot-three", true);
        final ActionFuture<CreateSnapshotResponse> snapshotFour = startFullSnapshot(repoName, "snapshot-four", true);
        awaitNumberOfSnapshotsInProgress(2);

        assertAcked(client().admin().indices().prepareDelete("index-two"));
        unblockNode(repoName, clusterManagerNode);

        assertThat(snapshotThree.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertThat(snapshotFour.get().getSnapshotInfo().state(), is(SnapshotState.PARTIAL));
        assertAcked(deleteFuture.get());
    }

    public void testQueuedSnapshotsWaitingForShardReady() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        final String repoName = "test-repo";
        createRepository(repoName, "fs");

        final String testIndex = "test-idx";
        // Create index on two nodes and make sure each node has a primary by setting no replicas
        assertAcked(prepareCreate(testIndex, 2, indexSettingsNoReplicas(between(2, 10))));

        ensureGreen(testIndex);

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            index(testIndex, "_doc", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertThat(client().prepareSearch(testIndex).setSize(0).get().getHits().getTotalHits().value, equalTo(100L));

        logger.info("--> start relocations");
        allowNodes(testIndex, 1);

        logger.info("--> wait for relocations to start");
        assertBusy(
            () -> assertThat(
                client().admin().cluster().prepareHealth(testIndex).execute().actionGet().getRelocatingShards(),
                greaterThan(0)
            ),
            1L,
            TimeUnit.MINUTES
        );

        logger.info("--> start two snapshots");
        final String snapshotOne = "snap-1";
        final String snapshotTwo = "snap-2";
        final ActionFuture<CreateSnapshotResponse> snapOneResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotOne)
            .setWaitForCompletion(false)
            .setIndices(testIndex)
            .execute();
        final ActionFuture<CreateSnapshotResponse> snapTwoResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotTwo)
            .setWaitForCompletion(false)
            .setIndices(testIndex)
            .execute();

        snapOneResponse.get();
        snapTwoResponse.get();
        logger.info("--> wait for snapshot to complete");
        for (String snapshot : Arrays.asList(snapshotOne, snapshotTwo)) {
            SnapshotInfo snapshotInfo = waitForCompletion(repoName, snapshot, TimeValue.timeValueSeconds(600));
            assertThat(snapshotInfo.state(), equalTo(SnapshotState.SUCCESS));
            assertThat(snapshotInfo.shardFailures().size(), equalTo(0));
        }
    }

    public void testBackToBackQueuedDeletes() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final List<String> snapshots = createNSnapshots(repoName, 2);
        final String snapshotOne = snapshots.get(0);
        final String snapshotTwo = snapshots.get(1);

        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startAndBlockOnDeleteSnapshot(repoName, snapshotOne);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotTwo = startDeleteSnapshot(repoName, snapshotTwo);
        awaitNDeletionsInProgress(2);

        unblockNode(repoName, clusterManagerName);
        assertAcked(deleteSnapshotOne.get());
        assertAcked(deleteSnapshotTwo.get());

        final RepositoryData repositoryData = getRepositoryData(repoName);
        assertThat(repositoryData.getSnapshotIds(), empty());
        // Two snapshots and two distinct delete operations move us 4 steps from -1 to 3
        assertThat(repositoryData.getGenId(), is(3L));
    }

    public void testQueuedOperationsAfterFinalizationFailure() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));

        final ActionFuture<CreateSnapshotResponse> snapshotThree = startAndBlockFailingFullSnapshot(repoName, "snap-other");

        final String clusterManagerName = internalCluster().getClusterManagerName();

        final String snapshotOne = snapshotNames.get(0);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startDeleteSnapshot(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);

        unblockNode(repoName, clusterManagerName);

        expectThrows(SnapshotException.class, snapshotThree::actionGet);
        assertAcked(deleteSnapshotOne.get());
    }

    public void testStartDeleteDuringFinalizationCleanup() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        createNSnapshots(repoName, randomIntBetween(1, 5));
        final String snapshotName = "snap-name";
        blockClusterManagerFromDeletingIndexNFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, snapshotName);
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, clusterManagerName);
        assertSuccessful(snapshotFuture);
        assertAcked(deleteFuture.get(30L, TimeUnit.SECONDS));
    }

    public void testEquivalentDeletesAreDeduplicated() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        createNSnapshots(repoName, randomIntBetween(1, 5));

        blockNodeOnAnyFiles(repoName, clusterManagerName);
        final int deletes = randomIntBetween(2, 10);
        final List<ActionFuture<AcknowledgedResponse>> deleteResponses = new ArrayList<>(deletes);
        for (int i = 0; i < deletes; ++i) {
            deleteResponses.add(client().admin().cluster().prepareDeleteSnapshot(repoName, "*").execute());
        }
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNDeletionsInProgress(1);
        for (ActionFuture<AcknowledgedResponse> deleteResponse : deleteResponses) {
            assertFalse(deleteResponse.isDone());
        }
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, clusterManagerName);
        for (ActionFuture<AcknowledgedResponse> deleteResponse : deleteResponses) {
            assertAcked(deleteResponse.get());
        }
    }

    public void testClusterManagerFailoverOnFinalizationLoop() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");
        final NetworkDisruption networkDisruption = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(networkDisruption);

        final List<String> snapshotNames = createNSnapshots(repoName, randomIntBetween(2, 5));
        final String clusterManagerName = internalCluster().getClusterManagerName();
        blockClusterManagerFromDeletingIndexNFile(repoName);
        final ActionFuture<CreateSnapshotResponse> snapshotThree = startFullSnapshotFromClusterManagerClient(repoName, "snap-other");
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));

        final String snapshotOne = snapshotNames.get(0);
        final ActionFuture<AcknowledgedResponse> deleteSnapshotOne = startDeleteSnapshot(repoName, snapshotOne);
        awaitNDeletionsInProgress(1);
        networkDisruption.startDisrupting();
        ensureStableCluster(3, dataNode);

        unblockNode(repoName, clusterManagerName);
        networkDisruption.stopDisrupting();
        ensureStableCluster(4);

        assertSuccessful(snapshotThree);
        try {
            deleteSnapshotOne.actionGet();
        } catch (RepositoryException re) {
            // ignored
        }
        awaitNoMoreRunningOperations();
    }

    public void testStatusMultipleSnapshotsMultipleRepos() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture1 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture2 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot-2",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture3 = startFullSnapshotBlockedOnDataNode(
            "other-blocked-snapshot",
            otherBlockedRepoName,
            dataNode
        );
        awaitNumberOfSnapshotsInProgress(3);

        assertSnapshotStatusCountOnRepo("_all", 3);
        assertSnapshotStatusCountOnRepo(blockedRepoName, 2);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(blockedRepoName, dataNode);
        awaitNumberOfSnapshotsInProgress(1);
        assertSnapshotStatusCountOnRepo("_all", 1);
        assertSnapshotStatusCountOnRepo(blockedRepoName, 0);
        assertSnapshotStatusCountOnRepo(otherBlockedRepoName, 1);

        unblockNode(otherBlockedRepoName, dataNode);
        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    public void testInterleavedAcrossMultipleRepos() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        // We're blocking a some of the snapshot threads when we block the first repo below so we have to make sure we have enough threads
        // left for the second concurrent snapshot.
        final String dataNode = startDataNodeWithLargeSnapshotPool();
        final String blockedRepoName = "test-repo-blocked-1";
        final String otherBlockedRepoName = "test-repo-blocked-2";
        createRepository(blockedRepoName, "mock");
        createRepository(otherBlockedRepoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSlowFuture1 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture2 = startFullSnapshotBlockedOnDataNode(
            "blocked-snapshot-2",
            blockedRepoName,
            dataNode
        );
        final ActionFuture<CreateSnapshotResponse> createSlowFuture3 = startFullSnapshotBlockedOnDataNode(
            "other-blocked-snapshot",
            otherBlockedRepoName,
            dataNode
        );
        awaitNumberOfSnapshotsInProgress(3);
        unblockNode(blockedRepoName, dataNode);
        unblockNode(otherBlockedRepoName, dataNode);

        assertSuccessful(createSlowFuture1);
        assertSuccessful(createSlowFuture2);
        assertSuccessful(createSlowFuture3);
    }

    public void testClusterManagerFailoverAndMultipleQueuedUpSnapshotsAcrossTwoRepos() throws Exception {
        disableRepoConsistencyCheck("This test corrupts the repository on purpose");

        internalCluster().startClusterManagerOnlyNodes(3, LARGE_SNAPSHOT_POOL_SETTINGS);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        final String otherRepoName = "other-test-repo";
        final Path repoPath = randomRepoPath();
        createRepository(repoName, "mock", repoPath);
        createRepository(otherRepoName, "mock");
        createIndexWithContent("index-one");
        createNSnapshots(repoName, randomIntBetween(2, 5));
        final int countOtherRepo = randomIntBetween(2, 5);
        createNSnapshots(otherRepoName, countOtherRepo);

        corruptIndexN(repoPath, getRepositoryData(repoName).getGenId());

        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(otherRepoName);

        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-blocked-1").setWaitForCompletion(false).get();
        client().admin().cluster().prepareCreateSnapshot(repoName, "snapshot-blocked-2").setWaitForCompletion(false).get();
        client().admin().cluster().prepareCreateSnapshot(otherRepoName, "snapshot-other-blocked-1").setWaitForCompletion(false).get();
        client().admin().cluster().prepareCreateSnapshot(otherRepoName, "snapshot-other-blocked-2").setWaitForCompletion(false).get();

        awaitNumberOfSnapshotsInProgress(4);
        final String initialClusterManager = internalCluster().getClusterManagerName();
        waitForBlock(initialClusterManager, repoName, TimeValue.timeValueSeconds(30L));
        waitForBlock(initialClusterManager, otherRepoName, TimeValue.timeValueSeconds(30L));

        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(3, dataNode);
        awaitNoMoreRunningOperations();

        final RepositoryData repositoryData = getRepositoryData(otherRepoName);
        assertThat(repositoryData.getSnapshotIds(), hasSize(countOtherRepo + 2));
    }

    public void testConcurrentOperationsLimit() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("index-test");

        final int limitToTest = randomIntBetween(1, 3);
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().put(SnapshotsService.MAX_CONCURRENT_SNAPSHOT_OPERATIONS_SETTING.getKey(), limitToTest).build()
                )
                .get()
        );

        final List<String> snapshotNames = createNSnapshots(repoName, limitToTest + 1);
        blockNodeOnAnyFiles(repoName, clusterManagerName);
        int blockedSnapshots = 0;
        boolean blockedDelete = false;
        final List<ActionFuture<CreateSnapshotResponse>> snapshotFutures = new ArrayList<>();
        ActionFuture<AcknowledgedResponse> deleteFuture = null;
        for (int i = 0; i < limitToTest; ++i) {
            if (blockedDelete || randomBoolean()) {
                snapshotFutures.add(startFullSnapshot(repoName, "snap-" + i));
                ++blockedSnapshots;
            } else {
                blockedDelete = true;
                deleteFuture = startDeleteSnapshot(repoName, randomFrom(snapshotNames));
            }
        }
        awaitNumberOfSnapshotsInProgress(blockedSnapshots);
        if (blockedDelete) {
            awaitNDeletionsInProgress(1);
        }
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));

        final String expectedFailureMessage = "Cannot start another operation, already running ["
            + limitToTest
            + "] operations and the current limit for concurrent snapshot operations is set to ["
            + limitToTest
            + "]";
        final ConcurrentSnapshotExecutionException csen1 = expectThrows(
            ConcurrentSnapshotExecutionException.class,
            () -> client().admin().cluster().prepareCreateSnapshot(repoName, "expected-to-fail").execute().actionGet()
        );
        assertThat(csen1.getMessage(), containsString(expectedFailureMessage));
        if (blockedDelete == false || limitToTest == 1) {
            final ConcurrentSnapshotExecutionException csen2 = expectThrows(
                ConcurrentSnapshotExecutionException.class,
                () -> client().admin().cluster().prepareDeleteSnapshot(repoName, "*").execute().actionGet()
            );
            assertThat(csen2.getMessage(), containsString(expectedFailureMessage));
        }

        unblockNode(repoName, clusterManagerName);
        if (deleteFuture != null) {
            assertAcked(deleteFuture.get());
        }
        for (ActionFuture<CreateSnapshotResponse> snapshotFuture : snapshotFutures) {
            assertSuccessful(snapshotFuture);
        }
    }

    public void testQueuedDeleteAfterFinalizationFailure() throws Exception {
        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(repoName);
        final String snapshotName = "snap-1";
        final ActionFuture<CreateSnapshotResponse> snapshotFuture = startFullSnapshot(repoName, snapshotName);
        waitForBlock(clusterManagerNode, repoName, TimeValue.timeValueSeconds(30L));
        final ActionFuture<AcknowledgedResponse> deleteFuture = startDeleteSnapshot(repoName, snapshotName);
        awaitNDeletionsInProgress(1);
        unblockNode(repoName, clusterManagerNode);
        assertAcked(deleteFuture.get());
        final SnapshotException sne = expectThrows(SnapshotException.class, snapshotFuture::actionGet);
        assertThat(sne.getCause().getMessage(), containsString("exception after block"));
    }

    public void testAbortNotStartedSnapshotWithoutIO() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");
        createIndexWithContent("test-index");

        final ActionFuture<CreateSnapshotResponse> createSnapshot1Future = startFullSnapshotBlockedOnDataNode(
            "first-snapshot",
            repoName,
            dataNode
        );

        final String snapshotTwo = "second-snapshot";
        final ActionFuture<CreateSnapshotResponse> createSnapshot2Future = startFullSnapshot(repoName, snapshotTwo);

        awaitNumberOfSnapshotsInProgress(2);

        assertAcked(startDeleteSnapshot(repoName, snapshotTwo).get());
        final SnapshotException sne = expectThrows(SnapshotException.class, createSnapshot2Future::actionGet);

        assertFalse(createSnapshot1Future.isDone());
        unblockNode(repoName, dataNode);
        assertSuccessful(createSnapshot1Future);
        assertThat(getRepositoryData(repoName).getGenId(), is(0L));
    }

    public void testStartWithSuccessfulShardSnapshotPendingFinalization() throws Exception {
        final String clusterManagerName = internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode();
        final String repoName = "test-repo";
        createRepository(repoName, "mock");

        createIndexWithContent("test-idx");
        createFullSnapshot(repoName, "first-snapshot");

        blockClusterManagerOnWriteIndexFile(repoName);
        final ActionFuture<CreateSnapshotResponse> blockedSnapshot = startFullSnapshot(repoName, "snap-blocked");
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        awaitNumberOfSnapshotsInProgress(1);
        blockNodeOnAnyFiles(repoName, dataNode);
        final ActionFuture<CreateSnapshotResponse> otherSnapshot = startFullSnapshot(repoName, "other-snapshot");
        awaitNumberOfSnapshotsInProgress(2);
        assertFalse(blockedSnapshot.isDone());
        unblockNode(repoName, clusterManagerName);
        awaitNumberOfSnapshotsInProgress(1);

        awaitClusterManagerFinishRepoOperations();

        unblockNode(repoName, dataNode);
        assertSuccessful(blockedSnapshot);
        assertSuccessful(otherSnapshot);
    }

    private static String startDataNodeWithLargeSnapshotPool() {
        return internalCluster().startDataOnlyNode(LARGE_SNAPSHOT_POOL_SETTINGS);
    }

    private static void assertSnapshotStatusCountOnRepo(String otherBlockedRepoName, int count) {
        final SnapshotsStatusResponse snapshotsStatusResponse = client().admin()
            .cluster()
            .prepareSnapshotStatus(otherBlockedRepoName)
            .get();
        final List<SnapshotStatus> snapshotStatuses = snapshotsStatusResponse.getSnapshots();
        assertThat(snapshotStatuses, hasSize(count));
    }

    private List<String> createNSnapshots(String repoName, int count) {
        final List<String> snapshotNames = new ArrayList<>(count);
        final String prefix = "snap-" + UUIDs.randomBase64UUID(random()).toLowerCase(Locale.ROOT) + "-";
        for (int i = 0; i < count; i++) {
            final String name = prefix + i;
            createFullSnapshot(repoName, name);
            snapshotNames.add(name);
        }
        logger.info("--> created {} in [{}]", snapshotNames, repoName);
        return snapshotNames;
    }

    private ActionFuture<AcknowledgedResponse> startDeleteFromNonClusterManagerClient(String repoName, String snapshotName) {
        logger.info("--> deleting snapshot [{}] from repo [{}] from non cluster-manager client", snapshotName, repoName);
        return internalCluster().nonClusterManagerClient().admin().cluster().prepareDeleteSnapshot(repoName, snapshotName).execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromNonClusterManagerClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from non cluster-manager client", snapshotName, repoName);
        return internalCluster().nonClusterManagerClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .execute();
    }

    private ActionFuture<CreateSnapshotResponse> startFullSnapshotFromClusterManagerClient(String repoName, String snapshotName) {
        logger.info("--> creating full snapshot [{}] to repo [{}] from cluster-manager client", snapshotName, repoName);
        return internalCluster().clusterManagerClient()
            .admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapshotName)
            .setWaitForCompletion(true)
            .execute();
    }

    private void createIndexWithContent(String indexName, String nodeInclude, String nodeExclude) {
        createIndexWithContent(
            indexName,
            indexSettingsNoReplicas(1).put("index.routing.allocation.include._name", nodeInclude)
                .put("index.routing.allocation.exclude._name", nodeExclude)
                .build()
        );
    }

    private static boolean snapshotHasCompletedShard(String snapshot, SnapshotsInProgress snapshotsInProgress) {
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                for (final SnapshotsInProgress.ShardSnapshotStatus shard : entry.shards().values()) {
                    if (shard.state().completed()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void corruptIndexN(Path repoPath, long generation) throws IOException {
        logger.info("--> corrupting [index-{}] in [{}]", generation, repoPath);
        Path indexNBlob = repoPath.resolve(BlobStoreRepository.INDEX_FILE_PREFIX + generation);
        assertFileExists(indexNBlob);
        Files.write(indexNBlob, randomByteArrayOfLength(1), StandardOpenOption.TRUNCATE_EXISTING);
    }

    private void awaitNDeletionsInProgress(int count) throws Exception {
        logger.info("--> wait for [{}] deletions to show up in the cluster state", count);
        awaitClusterState(
            state -> state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries().size() == count
        );
    }

    private static List<SnapshotInfo> currentSnapshots(String repoName) {
        return client().admin()
            .cluster()
            .prepareGetSnapshots(repoName)
            .setSnapshots(GetSnapshotsRequest.CURRENT_SNAPSHOT)
            .get()
            .getSnapshots();
    }

    private ActionFuture<AcknowledgedResponse> startAndBlockOnDeleteSnapshot(String repoName, String snapshotName)
        throws InterruptedException {
        final String clusterManagerName = internalCluster().getClusterManagerName();
        blockNodeOnAnyFiles(repoName, clusterManagerName);
        final ActionFuture<AcknowledgedResponse> fut = startDeleteSnapshot(repoName, snapshotName);
        waitForBlock(clusterManagerName, repoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private ActionFuture<CreateSnapshotResponse> startAndBlockFailingFullSnapshot(String blockedRepoName, String snapshotName)
        throws InterruptedException {
        blockClusterManagerFromFinalizingSnapshotOnIndexFile(blockedRepoName);
        final ActionFuture<CreateSnapshotResponse> fut = startFullSnapshot(blockedRepoName, snapshotName);
        waitForBlock(internalCluster().getClusterManagerName(), blockedRepoName, TimeValue.timeValueSeconds(30L));
        return fut;
    }

    private static void waitUntilInprogress(final String repoName, final String snapshotName, TimeValue timeout)
        throws InterruptedException {
        waitUntil(
            () -> currentSnapshots(repoName).stream()
                .filter(s -> s.snapshotId().getName().equals(snapshotName))
                .filter(s -> s.state() == SnapshotState.IN_PROGRESS)
                .findAny()
                .isPresent(),
            timeout.millis(),
            TimeUnit.MILLISECONDS
        );
    }
}
