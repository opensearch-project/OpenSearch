/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.StepListener;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.close.CloseIndexResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class RestoreServiceIntegTests extends OpenSearchSingleNodeTestCase {
    private final String indexName = "index_1";
    private final String renamedIndexName = "index_2";
    private final String aliasName = "alias_1";
    private final String renamedAliasName = "alias_2";
    private final String repoName = "repo_1";
    private final String snapShotName = "snap_1";
    private final int waitInSeconds = 60;
    private boolean exists;
    private boolean closed;
    private boolean includeAlias;
    private boolean renameAliases;
    private boolean renameIndexes;

    public RestoreServiceIntegTests(TestCase testCase) {
        this.exists = testCase.exists;
        this.closed = testCase.closed;
        this.includeAlias = testCase.includeAlias;
        this.renameAliases = testCase.renameAliases;
        this.renameIndexes = testCase.renameIndexes;
    }

    public static class TestCase {
        public boolean exists;
        public boolean closed;
        public boolean includeAlias;
        public boolean renameAliases;
        public boolean renameIndexes;

        public TestCase(boolean exists, boolean closed, boolean includeAlias, boolean renameAliases, boolean renameIndexes) {
            this.exists = exists;
            this.closed = closed;
            this.includeAlias = includeAlias;
            this.renameAliases = renameAliases;
            this.renameIndexes = renameIndexes;
        }

        public String toString() {
            return String.join(
                " and ",
                new String[] {
                    exists ? "target index exists and is" + (closed ? "closed" : "open") : "doesn't exist",
                    includeAlias ? "including aliases" : "not including aliases",
                    renameIndexes ? "renaming indexes" : "not renaming indexes",
                    renameAliases ? "renaming aliases" : "not renaming aliases" }
            );
        }
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { new TestCase(false, false, true, true, true) },
            new Object[] { new TestCase(false, false, false, true, true) },
            new Object[] { new TestCase(false, false, true, false, false) },
            new Object[] { new TestCase(false, false, false, false, false) },
            new Object[] { new TestCase(true, false, true, true, true) },
            new Object[] { new TestCase(true, false, false, true, true) },
            new Object[] { new TestCase(true, true, true, true, true) },
            new Object[] { new TestCase(true, true, false, true, true) },
            new Object[] { new TestCase(true, false, false, false, false) },
            new Object[] { new TestCase(true, false, true, false, false) },
            new Object[] { new TestCase(true, true, false, false, false) },
            new Object[] { new TestCase(true, true, true, false, false) }
        );
    }

    @After
    public void cleanup() throws InterruptedException {
        final CountDownLatch allDeleted = new CountDownLatch(3);
        for (String indexName : new String[] { indexName, renamedIndexName }) {
            client().admin().indices().existsAsync(new IndicesExistsRequest(indexName)).thenAccept(resp -> {
                if (resp.isExists()) {
                    client().admin()
                        .indices()
                        .deleteAsync(new DeleteIndexRequest(indexName))
                        .thenAccept(ignored -> { allDeleted.countDown(); });
                } else {
                    allDeleted.countDown();
                }
            });
        }

        final StepListener<GetSnapshotsResponse> snapStatusResponseStepListener = new StepListener<>();
        client().admin().cluster().getSnapshots(new GetSnapshotsRequest(repoName), snapStatusResponseStepListener);
        continueOrDie(snapStatusResponseStepListener, resp -> {
            if (resp.getSnapshots().stream().anyMatch(s -> s.snapshotId().getName().equals(snapShotName))) {
                final StepListener<AcknowledgedResponse> deleteSnapResponseStepListener = new StepListener<>();
                client().admin()
                    .cluster()
                    .deleteSnapshot(new DeleteSnapshotRequest(repoName, snapShotName), deleteSnapResponseStepListener);
                continueOrDie(deleteSnapResponseStepListener, ignored -> allDeleted.countDown());
            } else {
                allDeleted.countDown();
            }
        });

        allDeleted.await(waitInSeconds, TimeUnit.SECONDS);
    }

    public void testRestoreWithRename() throws Exception {

        assert this.exists || !this.closed; // index close state doesn't exist when the index doesn't exist - so only permit one value of
                                            // closed to avoid pointless duplicate tests
        final boolean expectSuccess = !this.exists || this.closed;
        final int documents = randomIntBetween(1, 100);

        this.createIndex(indexName);
        if (this.exists && this.renameIndexes) {
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

        isDocumentFinished.await(waitInSeconds, TimeUnit.SECONDS);

        if (this.closed) {
            final CountDownLatch isClosed = new CountDownLatch(1);
            final StepListener<CloseIndexResponse> closeIndexResponseStepListener = new StepListener<>();
            final String indexToClose = this.renameIndexes ? renamedIndexName : indexName;
            client().admin().indices().close(new CloseIndexRequest(indexToClose), closeIndexResponseStepListener);

            continueOrDie(closeIndexResponseStepListener, ignored -> { isClosed.countDown(); });
            isClosed.await(waitInSeconds, TimeUnit.SECONDS);
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

        if (!this.exists && !this.renameIndexes) {
            continueOrDie(createSnapshotResponseStepListener, ignored -> {
                client().admin().indices().deleteAsync(new DeleteIndexRequest(indexName)).thenAccept(r -> isRestorable.countDown());
            });
        } else {
            continueOrDie(createSnapshotResponseStepListener, ignored -> isRestorable.countDown());
        }

        isRestorable.await(waitInSeconds, TimeUnit.SECONDS);

        final StepListener<RestoreSnapshotResponse> restoreSnapshotResponseStepListener = new StepListener<>();
        final CountDownLatch isRestored = new CountDownLatch(1);
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest(repoName, snapShotName).includeAliases(this.includeAlias)
            .waitForCompletion(true);
        if (this.renameAliases) {
            restoreSnapshotRequest = restoreSnapshotRequest.renameAliasPattern("1").renameAliasReplacement("2");
        }
        if (this.renameIndexes) {
            restoreSnapshotRequest = restoreSnapshotRequest.renamePattern("1").renameReplacement("2");
        }
        client().admin().cluster().restoreSnapshot(restoreSnapshotRequest, restoreSnapshotResponseStepListener);

        restoreSnapshotResponseStepListener.whenComplete(ignored -> {
            isRestored.countDown();
            assertTrue("unexpected successful restore", expectSuccess);
        }, e -> {
            isRestored.countDown();
            if (expectSuccess) {
                throw new RuntimeException(e);
            }
        });

        isRestored.await(waitInSeconds, TimeUnit.SECONDS);

        if (expectSuccess) {
            final String indexToSearch = this.renameIndexes ? renamedIndexName : indexName;
            final String aliasToSearch = this.renameAliases ? renamedAliasName : aliasName;

            if (this.closed) {
                final CountDownLatch isOpened = new CountDownLatch(1);
                final StepListener<OpenIndexResponse> openIndexResponseStepListener = new StepListener<>();
                client().admin().indices().open(new OpenIndexRequest(indexToSearch).waitForActiveShards(1), openIndexResponseStepListener);
                continueOrDie(openIndexResponseStepListener, ignored -> { isOpened.countDown(); });

                isOpened.await(waitInSeconds, TimeUnit.SECONDS);
            }

            final CountDownLatch isSearchDone = new CountDownLatch(this.includeAlias ? 2 : 1);
            final StepListener<SearchResponse> searchIndexResponseListener = new StepListener<>();
            final StepListener<SearchResponse> searchAliasResponseListener = new StepListener<>();
            client().search(
                new SearchRequest(indexToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                searchIndexResponseListener
            );
            continueOrDie(searchIndexResponseListener, ignored -> { isSearchDone.countDown(); });
            if (this.includeAlias) {
                client().search(
                    new SearchRequest(aliasToSearch).source(new SearchSourceBuilder().size(0).trackTotalHits(true)),
                    searchAliasResponseListener
                );
                continueOrDie(searchAliasResponseListener, ignored -> { isSearchDone.countDown(); });
            }

            isSearchDone.await(waitInSeconds, TimeUnit.SECONDS);

            assertEquals(documents, Objects.requireNonNull(searchIndexResponseListener.result().getHits().getTotalHits()).value());
            if (this.includeAlias) {
                assertEquals(documents, Objects.requireNonNull(searchAliasResponseListener.result().getHits().getTotalHits()).value());
            }
        }
    }

    private static <T> void continueOrDie(StepListener<T> listener, CheckedConsumer<T, Exception> onResponse) {
        listener.whenComplete(onResponse, e -> { throw new AssertionError(e); });
    }
}
