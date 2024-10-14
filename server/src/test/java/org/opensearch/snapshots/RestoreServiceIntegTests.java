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
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
//import static org.opensearch.node.Node.NODE_NAME_SETTING;

public class RestoreServiceIntegTests extends OpenSearchSingleNodeTestCase {
    // private DeterministicTaskQueue deterministicTaskQueue;

    // @Before
    // public void createServices() {
    // deterministicTaskQueue = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "shared").build(), random());
    // }

    // TODO there is certainly a better way to do this, but I don't know what it is....
    public void testRestoreToNewWithAliasAndRename() {
        __testRestoreWithRename(false, false, true, true, true);
    }

    public void testRestoreToNewWithoutAliasAndWithRename() {
        __testRestoreWithRename(false, false, false, true, true);
    }

    public void testRestoreOverExistingOpenWithAliasAndRename() {
        __testRestoreWithRename(true, false, true, true, true);
    }

    public void testRestoreOverExistingOpenWithoutAliasAndWithRename() {
        __testRestoreWithRename(true, false, false, true, true);
    }

    public void testRestoreOverExistingClosedWithAliasAndRename() {
        __testRestoreWithRename(true, true, true, true, true);
    }

    public void testRestoreOverExistingClosedWithoutAliasAndWithRename() {
        __testRestoreWithRename(true, true, false, true, true);
    }

    public void testRestoreOverExistingOpenWithoutAliasAndRename() {
        __testRestoreWithRename(true, false, false, false, false);
    }

    public void testRestoreOverExistingOpenWithAliasAndWithoutRename() {
        __testRestoreWithRename(true, false, true, false, false);
    }

    public void testRestoreOverExistingClosedWithoutAliasAndRename() {
        __testRestoreWithRename(true, true, false, false, false);
    }

    public void testRestoreOverExistingClosedWithAliasAndWithoutRename() {
        __testRestoreWithRename(true, true, true, false, false);
    }

    public void __testRestoreWithRename(
        boolean exists,
        boolean closed,
        boolean includeAlias,
        boolean renameAliases,
        boolean renameIndexes
    ) {
        assert exists || renameIndexes;
        final String indexName = "index_1";
        final String renamedIndexName = "index_2";
        final String aliasName = "alias_1";
        final String renamedAliasName = "alias_2";
        final String repoName = "repo_1";
        final String snapShotName = "snap_1";
        final boolean expectSuccess = !exists || closed;
        final int documents = randomIntBetween(1, 100);

        this.createIndex(indexName);
        if (exists && renameIndexes) {
            this.createIndex(renamedIndexName);
        }

        final StepListener<AcknowledgedResponse> createAliasResponseStepListener = new StepListener<>();
        client().admin()
            .indices()
            .aliases(
                new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().alias(aliasName).index(indexName)),
                createAliasResponseStepListener
            );

        final AtomicBoolean isDocumentFinished = new AtomicBoolean(false);
        continueOrDie(createAliasResponseStepListener, ignored -> {
            final BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int i = 0; i < documents; ++i) {
                bulkRequest.add(new IndexRequest(indexName).source(Collections.singletonMap("foo", "bar" + i)));
            }
            final StepListener<BulkResponse> bulkResponseStepListener = new StepListener<>();
            client().bulk(bulkRequest, bulkResponseStepListener);
            continueOrDie(bulkResponseStepListener, bulkResponse -> {
                assertFalse("Failures in bulk response: " + bulkResponse.buildFailureMessage(), bulkResponse.hasFailures());
                assertEquals(documents, bulkResponse.getItems().length);
                isDocumentFinished.set(true);
            });
        });

        Settings.Builder settings = Settings.builder().put("location", randomAlphaOfLength(10));
        OpenSearchIntegTestCase.putRepository(client().admin().cluster(), repoName, FsRepository.TYPE, settings);

        runUntil(isDocumentFinished::get, TimeUnit.MINUTES.toMillis(1L));

        if (closed) {
            final AtomicBoolean isReady = new AtomicBoolean(false);
            final StepListener<CloseIndexResponse> closeIndexResponseStepListener = new StepListener<>();
            final String indexToClose = renameIndexes ? renamedIndexName : indexName;
            client().admin().indices().close(new CloseIndexRequest(indexToClose), closeIndexResponseStepListener);

            continueOrDie(closeIndexResponseStepListener, ignored -> { isReady.set(true); });
            runUntil(isReady::get, TimeUnit.MINUTES.toMillis(1L));
        }

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
            RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repoName, snapShotName).includeAliases(includeAlias)
                .waitForCompletion(true);
            if (renameAliases) {
                restoreSnapshotRequest = restoreSnapshotRequest.renameAliasPattern("1").renameAliasReplacement("2");
            }
            if (renameIndexes) {
                restoreSnapshotRequest = restoreSnapshotRequest.renamePattern("1").renameReplacement("2");
            }
            client().admin().cluster().restoreSnapshot(restoreSnapshotRequest, restoreSnapshotResponseStepListener);
        });

        restoreSnapshotResponseStepListener.whenComplete(r -> {
            isFinished.set(true);
            assertTrue("unexpected sucesssful restore", expectSuccess);
        }, e -> {
            isFinished.set(true);
            if (expectSuccess) {
                throw new RuntimeException(e);
            }
        });

        runUntil(isFinished::get, TimeUnit.SECONDS.toMillis(10L));

        if (expectSuccess) {
            // assertEquals(shards, restoreSnapshotResponse.getRestoreInfo().totalShards());
            final String indexToSearch = renameIndexes ? renamedIndexName : indexName;
            final String aliasToSearch = renameAliases ? renamedAliasName : aliasName;

            if (closed) {
                final AtomicBoolean isReady = new AtomicBoolean(false);
                final StepListener<OpenIndexResponse> openIndexResponseStepListener = new StepListener<>();
                client().admin().indices().open(new OpenIndexRequest(indexToSearch).waitForActiveShards(1), openIndexResponseStepListener);
                continueOrDie(openIndexResponseStepListener, ignored -> { isReady.set(true); });
                runUntil(isReady::get, TimeUnit.MINUTES.toMillis(1L));
            }

            final StepListener<SearchResponse> searchIndexResponseListener = new StepListener<>();
            final StepListener<SearchResponse> searchAliasResponseListener = new StepListener<>();
            final int expectedCount = includeAlias ? 2 : 1;
            final AtomicInteger isSearchDone = new AtomicInteger(0);
            client().search(
                new SearchRequest(indexToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchIndexResponseListener
            );
            continueOrDie(searchIndexResponseListener, ignored -> { isSearchDone.addAndGet(1); });
            if (includeAlias) {
                client().search(
                    new SearchRequest(aliasToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                    searchAliasResponseListener
                );
                continueOrDie(searchAliasResponseListener, ignored -> { isSearchDone.addAndGet(1); });
            }

            runUntil(() -> { return isSearchDone.get() >= expectedCount; }, TimeUnit.SECONDS.toMillis(10L));

            assertEquals(documents, Objects.requireNonNull(searchIndexResponseListener.result().getHits().getTotalHits()).value);
            if (includeAlias) {
                assertEquals(documents, Objects.requireNonNull(searchAliasResponseListener.result().getHits().getTotalHits()).value);
            }
        }
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
