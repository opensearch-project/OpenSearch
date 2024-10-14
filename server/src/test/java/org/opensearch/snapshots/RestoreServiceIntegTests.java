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
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RestoreServiceIntegTests extends OpenSearchSingleNodeTestCase {
    // TODO there is certainly a better way to do this, but I don't know what it is....
    public void testRestoreToNewWithAliasAndRename() throws Exception {
        __testRestoreWithRename(false, false, true, true, true);
    }

    public void testRestoreToNewWithoutAliasAndWithRename() throws Exception {
        __testRestoreWithRename(false, false, false, true, true);
    }

    public void testRestoreToNewWithAliasAndWithoutRename() throws Exception {
        __testRestoreWithRename(false, false, true, false, false);
    }

    public void testRestoreToNewWithoutAliasAndRename() throws Exception {
        __testRestoreWithRename(false, false, false, false, false);
    }

    public void testRestoreOverExistingOpenWithAliasAndRename() throws Exception {
        __testRestoreWithRename(true, false, true, true, true);
    }

    public void testRestoreOverExistingOpenWithoutAliasAndWithRename() throws Exception {
        __testRestoreWithRename(true, false, false, true, true);
    }

    public void testRestoreOverExistingClosedWithAliasAndRename() throws Exception {
        __testRestoreWithRename(true, true, true, true, true);
    }

    public void testRestoreOverExistingClosedWithoutAliasAndWithRename() throws Exception {
        __testRestoreWithRename(true, true, false, true, true);
    }

    public void testRestoreOverExistingOpenWithoutAliasAndRename() throws Exception {
        __testRestoreWithRename(true, false, false, false, false);
    }

    public void testRestoreOverExistingOpenWithAliasAndWithoutRename() throws Exception {
        __testRestoreWithRename(true, false, true, false, false);
    }

    public void testRestoreOverExistingClosedWithoutAliasAndRename() throws Exception {
        __testRestoreWithRename(true, true, false, false, false);
    }

    public void testRestoreOverExistingClosedWithAliasAndWithoutRename() throws Exception {
        __testRestoreWithRename(true, true, true, false, false);
    }

    private void __testRestoreWithRename(boolean exists, boolean closed, boolean includeAlias, boolean renameAliases, boolean renameIndexes)
        throws Exception {
        assert exists || !closed;
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

        final StepListener<AcknowledgedResponse> putRepositoryResponseStepListener = new StepListener<>();
        Settings.Builder settings = Settings.builder().put("location", randomAlphaOfLength(10));
        OpenSearchIntegTestCase.putRepository(
            client().admin().cluster(),
            repoName,
            FsRepository.TYPE,
            settings,
            putRepositoryResponseStepListener
        );

        final StepListener<AcknowledgedResponse> createAliasResponseStepListener = new StepListener<>();
        client().admin()
            .indices()
            .aliases(
                new IndicesAliasesRequest().addAliasAction(IndicesAliasesRequest.AliasActions.add().alias(aliasName).index(indexName)),
                createAliasResponseStepListener
            );

        final CountDownLatch isDocumentFinished = new CountDownLatch(1);
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
                isDocumentFinished.countDown();
            });
        });

        isDocumentFinished.await(1, TimeUnit.MINUTES);

        if (closed) {
            final CountDownLatch isClosed = new CountDownLatch(1);
            final StepListener<CloseIndexResponse> closeIndexResponseStepListener = new StepListener<>();
            final String indexToClose = renameIndexes ? renamedIndexName : indexName;
            client().admin().indices().close(new CloseIndexRequest(indexToClose), closeIndexResponseStepListener);

            continueOrDie(closeIndexResponseStepListener, ignored -> { isClosed.countDown(); });
            isClosed.await(1, TimeUnit.MINUTES);
        }

        final StepListener<CreateSnapshotResponse> createSnapshotResponseStepListener = new StepListener<>();
        continueOrDie(putRepositoryResponseStepListener, ignored -> {
            client().admin()
                .cluster()
                .prepareCreateSnapshot(repoName, snapShotName)
                .setWaitForCompletion(true)
                .setPartial(true)
                .execute(createSnapshotResponseStepListener);
        });

        final CountDownLatch isRestorable = new CountDownLatch(1);

        if (!exists && !renameIndexes) {
            final StepListener<AcknowledgedResponse> deleteIndexResponseStepListener = new StepListener<>();
            continueOrDie(createSnapshotResponseStepListener, ignored -> {
                client().admin().indices().delete(new DeleteIndexRequest(indexName), deleteIndexResponseStepListener);
            });
            continueOrDie(deleteIndexResponseStepListener, ignored -> isRestorable.countDown());
        } else {
            continueOrDie(createSnapshotResponseStepListener, ignored -> isRestorable.countDown());
        }

        isRestorable.await(1, TimeUnit.MINUTES);

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new StepListener<>();
        final CountDownLatch isRestored = new CountDownLatch(1);
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repoName, snapShotName).includeAliases(includeAlias)
            .waitForCompletion(true);
        if (renameAliases) {
            restoreSnapshotRequest = restoreSnapshotRequest.renameAliasPattern("1").renameAliasReplacement("2");
        }
        if (renameIndexes) {
            restoreSnapshotRequest = restoreSnapshotRequest.renamePattern("1").renameReplacement("2");
        }
        client().admin().cluster().restoreSnapshot(restoreSnapshotRequest, restoreSnapshotResponseStepListener);

        restoreSnapshotResponseStepListener.whenComplete(ignored -> {
            isRestored.countDown();
            assertTrue("unexpected sucesssful restore", expectSuccess);
        }, e -> {
            isRestored.countDown();
            if (expectSuccess) {
                throw new RuntimeException(e);
            }
        });

        isRestored.await(1, TimeUnit.MINUTES);

        if (expectSuccess) {
            final String indexToSearch = renameIndexes ? renamedIndexName : indexName;
            final String aliasToSearch = renameAliases ? renamedAliasName : aliasName;

            if (closed) {
                final CountDownLatch isOpened = new CountDownLatch(1);
                final StepListener<OpenIndexResponse> openIndexResponseStepListener = new StepListener<>();
                client().admin().indices().open(new OpenIndexRequest(indexToSearch).waitForActiveShards(1), openIndexResponseStepListener);
                continueOrDie(openIndexResponseStepListener, ignored -> { isOpened.countDown(); });

                isOpened.await(1, TimeUnit.MINUTES);
            }

            final CountDownLatch isSearchDone = new CountDownLatch(includeAlias ? 2 : 1);
            final StepListener<SearchResponse> searchIndexResponseListener = new StepListener<>();
            final StepListener<SearchResponse> searchAliasResponseListener = new StepListener<>();
            client().search(
                new SearchRequest(indexToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchIndexResponseListener
            );
            continueOrDie(searchIndexResponseListener, ignored -> { isSearchDone.countDown(); });
            if (includeAlias) {
                client().search(
                    new SearchRequest(aliasToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                    searchAliasResponseListener
                );
                continueOrDie(searchAliasResponseListener, ignored -> { isSearchDone.countDown(); });
            }

            isSearchDone.await(1, TimeUnit.MINUTES);

            assertEquals(documents, Objects.requireNonNull(searchIndexResponseListener.result().getHits().getTotalHits()).value);
            if (includeAlias) {
                assertEquals(documents, Objects.requireNonNull(searchAliasResponseListener.result().getHits().getTotalHits()).value);
            }
        }
    }

    private static <T> void continueOrDie(StepListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.whenComplete(onResponse, e -> { throw new AssertionError(e); });
    }
}
