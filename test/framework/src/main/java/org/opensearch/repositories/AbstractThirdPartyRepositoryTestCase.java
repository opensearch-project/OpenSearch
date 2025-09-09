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

package org.opensearch.repositories;

import org.opensearch.action.ActionRunnable;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.BlobStoreTestUtil;
import org.opensearch.snapshots.SnapshotState;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public abstract class AbstractThirdPartyRepositoryTestCase extends OpenSearchSingleNodeTestCase {
    private String repositoryName;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).setSecureSettings(credentials()).build();
    }

    protected abstract SecureSettings credentials();

    protected abstract void createRepository(String repoName);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        repositoryName = "test-repo-" + randomAlphaOfLength(8);
        createRepository(repositoryName);
        deleteAndAssertEmpty(getRepository().basePath());
    }

    @Override
    public void tearDown() throws Exception {
        deleteAndAssertEmpty(getRepository().basePath());
        client().admin().cluster().prepareDeleteRepository(repositoryName).get();
        super.tearDown();
    }

    private void deleteAndAssertEmpty(BlobPath path) throws Exception {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        repo.threadPool().generic().execute(ActionRunnable.run(future, () -> repo.blobStore().blobContainer(path).delete()));
        future.actionGet();
        final BlobPath parent = path.parent();
        if (parent == null) {
            assertChildren(path, Collections.emptyList());
        } else {
            assertDeleted(parent, path.toArray()[path.toArray().length - 1]);
        }
    }

    public void testCreateSnapshot() {
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            client().admin().cluster().prepareGetSnapshots(repositoryName).setSnapshots(snapshotName).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );

        assertTrue(client().admin().cluster().prepareDeleteSnapshot(repositoryName, snapshotName).get().isAcknowledged());
    }

    public void testListChildren() throws Exception {
        final BlobStoreRepository repo = getRepository();
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        final Executor genericExec = repo.threadPool().generic();
        final int testBlobLen = randomIntBetween(1, 100);
        genericExec.execute(ActionRunnable.run(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            blobStore.blobContainer(repo.basePath().add("foo"))
                .writeBlob("nested-blob", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
            blobStore.blobContainer(repo.basePath().add("foo").add("nested"))
                .writeBlob("bar", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
            blobStore.blobContainer(repo.basePath().add("foo").add("nested2"))
                .writeBlob("blub", new ByteArrayInputStream(randomByteArrayOfLength(testBlobLen)), testBlobLen, false);
        }));
        future.actionGet();
        assertChildren(repo.basePath(), Collections.singleton("foo"));
        assertBlobsByPrefix(repo.basePath(), "fo", Collections.emptyMap());
        assertChildren(repo.basePath().add("foo"), Arrays.asList("nested", "nested2"));
        assertBlobsByPrefix(
            repo.basePath().add("foo"),
            "nest",
            Collections.singletonMap("nested-blob", new PlainBlobMetadata("nested-blob", testBlobLen))
        );
        assertChildren(repo.basePath().add("foo").add("nested"), Collections.emptyList());
        if (randomBoolean()) {
            deleteAndAssertEmpty(repo.basePath());
        } else {
            deleteAndAssertEmpty(repo.basePath().add("foo"));
        }
    }

    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetadata> blobs) throws Exception {
        BlobStoreTestUtil.assertBlobsByPrefix(getRepository(), path, prefix, blobs);
    }

    public void testCleanup() throws Exception {
        createIndex("test-idx-1");
        createIndex("test-idx-2");
        createIndex("test-idx-3");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test-idx-1").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-2").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
            client().prepareIndex("test-idx-3").setId(Integer.toString(i)).setSource("foo", "bar" + i).get();
        }
        client().admin().indices().prepareRefresh().get();

        final String snapshotName = "test-snap-" + System.currentTimeMillis();

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices("test-idx-*", "-test-idx-3")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertThat(
            client().admin().cluster().prepareGetSnapshots(repositoryName).setSnapshots(snapshotName).get().getSnapshots().get(0).state(),
            equalTo(SnapshotState.SUCCESS)
        );

        final BlobStoreRepository repo = (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(repositoryName);
        final Executor genericExec = repo.threadPool().executor(ThreadPool.Names.GENERIC);

        logger.info("--> creating a dangling index folder");

        createDanglingIndex(repo, genericExec);

        logger.info("--> deleting a snapshot to trigger repository cleanup");
        client().admin().cluster().deleteSnapshot(new DeleteSnapshotRequest(repositoryName, snapshotName)).actionGet();

        assertConsistentRepository(repo, genericExec);

        logger.info("--> Create dangling index");
        createDanglingIndex(repo, genericExec);

        logger.info("--> Execute repository cleanup");
        final CleanupRepositoryResponse response = client().admin().cluster().prepareCleanupRepository(repositoryName).get();
        assertCleanupResponse(response, 3L, 1L);
    }

    protected void assertCleanupResponse(CleanupRepositoryResponse response, long bytes, long blobs) {
        assertThat(response.result().blobs(), equalTo(1L + 2L));
        assertThat(response.result().bytes(), equalTo(3L + 2 * 3L));
    }

    private void createDanglingIndex(final BlobStoreRepository repo, final Executor genericExec) throws Exception {
        final PlainActionFuture<Void> future = PlainActionFuture.newFuture();
        genericExec.execute(ActionRunnable.run(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            blobStore.blobContainer(repo.basePath().add("indices").add("foo"))
                .writeBlob("bar", new ByteArrayInputStream(new byte[3]), 3, false);
            for (String prefix : Arrays.asList("snap-", "meta-")) {
                blobStore.blobContainer(repo.basePath()).writeBlob(prefix + "foo.dat", new ByteArrayInputStream(new byte[3]), 3, false);
            }
        }));
        future.actionGet();
        assertTrue(assertCorruptionVisible(repo, genericExec));
    }

    protected boolean assertCorruptionVisible(BlobStoreRepository repo, Executor executor) throws Exception {
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        executor.execute(ActionRunnable.supply(future, () -> {
            final BlobStore blobStore = repo.blobStore();
            return blobStore.blobContainer(repo.basePath().add("indices")).children().containsKey("foo")
                && blobStore.blobContainer(repo.basePath().add("indices").add("foo")).blobExists("bar")
                && blobStore.blobContainer(repo.basePath()).blobExists("meta-foo.dat")
                && blobStore.blobContainer(repo.basePath()).blobExists("snap-foo.dat");
        }));
        return future.actionGet();
    }

    protected void assertConsistentRepository(BlobStoreRepository repo, Executor executor) throws Exception {
        BlobStoreTestUtil.assertConsistency(repo, executor);
    }

    protected void assertDeleted(BlobPath path, String name) throws Exception {
        assertThat(listChildren(path), not(contains(name)));
    }

    protected void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        listChildren(path);
        final Set<String> foundChildren = listChildren(path);
        if (children.isEmpty()) {
            assertThat(foundChildren, empty());
        } else {
            assertThat(foundChildren, containsInAnyOrder(children.toArray(Strings.EMPTY_ARRAY)));
        }
    }

    private Set<String> listChildren(BlobPath path) {
        final PlainActionFuture<Set<String>> future = PlainActionFuture.newFuture();
        final BlobStoreRepository repository = getRepository();
        repository.threadPool()
            .generic()
            .execute(ActionRunnable.supply(future, () -> repository.blobStore().blobContainer(path).children().keySet()));
        return future.actionGet();
    }

    protected BlobStoreRepository getRepository() {
        return (BlobStoreRepository) getInstanceFromNode(RepositoriesService.class).repository(repositoryName);
    }
}
