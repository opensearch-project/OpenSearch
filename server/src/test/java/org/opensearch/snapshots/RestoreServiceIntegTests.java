/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
//import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class RestoreServiceIntegTests extends OpenSearchSingleNodeTestCase {
    // private DeterministicTaskQueue deterministicTaskQueue;

    // @Before
    // public void createServices() {
    // deterministicTaskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "shared").build(), random());
    // }

    // TODO there is certainly a better way to do this, but I don't know what it is....
    public void testRestoreWithAliasAndRename() {
        __testRestoreWithRename(false, true, true, true);
    }

    public void testRestoreWithoutAliasAndWithRename() {
        __testRestoreWithRename(false, false, true, true);
    }

    public void testRestoreWithoutAliasAndRename() {
        __testRestoreWithRename(false, false, false, false);
    }

    public void testRestoreWithAliasAndWithoutRename() {
        __testRestoreWithRename(false, true, false, false);
    }

    public void __testRestoreWithRename(boolean closed, boolean includeAlias, boolean renameAlias, boolean renameIndexes) {
        final String indexName = "index_1";
        final String aliasName = "alias_1";
        final String repoName = "repo_1";
        final String snapShotName = "snap_1";
        this.createIndex(indexName);
        client().admin()
            .indices()
            .aliases(
                new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().alias(aliasName).index(indexName))
            );

        final boolean expectFailure = !renameIndexes || (includeAlias && !renameAlias);

        Settings.Builder settings = Settings.builder().put("location", randomAlphaOfLength(10));
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), repoName, FsRepository.TYPE, settings);

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();

        client().admin()
            .cluster()
            .prepareCreateSnapshot(repoName, snapShotName)
            .setWaitForCompletion(true)
            .setPartial(true)
            .execute(createSnapshotResponseStepListener);

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new StepListener<>();

        final AtomicBoolean isFinished = new AtomicBoolean(false);
        continueOrDie(createSnapshotResponseStepListener, r -> {
            assert r.getSnapshotInfo().state() == SnapshotState.SUCCESS;
            RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repoName, snapShotName).includeAliases(includeAlias);
            if (renameAlias) {
                restoreSnapshotRequest = restoreSnapshotRequest.renameAliasPattern("1").renameAliasReplacement("2");
            }
            if (renameIndexes) {
                restoreSnapshotRequest = restoreSnapshotRequest.renamePattern("1").renameReplacement("2");
            }
            client().admin().cluster().restoreSnapshot(restoreSnapshotRequest, restoreSnapshotResponseStepListener);
        });

        restoreSnapshotResponseStepListener.whenComplete(r -> {
            assertFalse("unexpected sucesssful restore", expectFailure);
            isFinished.set(true);
        }, e -> {
            if (expectFailure) {
                // expected failed - ignore
                isFinished.set(true);
            } else {
                throw new RuntimeException(e);
            }
        });

        runUntil(isFinished::get, TimeUnit.MINUTES.toMillis(1L));
    }

    private static <T> void continueOrDie(StepListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.whenComplete(onResponse, e -> { throw new AssertionError(e); });
    }

    // TODO there is certainly a better way to do this, but I don't know what it is....
    private void runUntil(Supplier<Boolean> fulfilled, long timeout) {
        // final long start = deterministicTaskQueue.getCurrentTimeMillis();
        // while (timeout > deterministicTaskQueue.getCurrentTimeMillis() - start) {
        final long start = System.currentTimeMillis();
        while (timeout > System.currentTimeMillis() - start) {
            if (fulfilled.get()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {}
            // deterministicTaskQueue.runAllRunnableTasks();
            // deterministicTaskQueue.advanceTime();
        }
        fail("Condition wasn't fulfilled.");
    }
}
