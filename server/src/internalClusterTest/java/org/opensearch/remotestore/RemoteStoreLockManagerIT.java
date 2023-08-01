/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.junit.Before;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.lockmanager.FileLockInfo;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;

public class RemoteStoreLockManagerIT extends RemoteStoreBaseIntegTestCase {
    private static final String TEST_METADATA = "test_metadata";
    private static final String INDEX_NAME = "remote-store-test-idx-1";

    private RemoteStoreMetadataLockManager remoteStoreMetadataLockManager;
    private RemoteSegmentStoreDirectory remoteSegmentStoreDirectory;
    private String indexUUID;

    @Before
    public void setup() throws IOException {
        setupRepo();
        RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(1));
        indexUUID = client().admin()
            .indices()
            .prepareGetSettings(INDEX_NAME)
            .get()
            .getSetting(INDEX_NAME, IndexMetadata.SETTING_INDEX_UUID);
        remoteStoreMetadataLockManager = RemoteStoreLockManagerFactory.newLockManager(repositoriesService, REPOSITORY_NAME, indexUUID, "0");
        remoteSegmentStoreDirectory = (RemoteSegmentStoreDirectory) new RemoteSegmentStoreDirectoryFactory(
            () -> repositoriesService,
            threadPool
        ).newDirectory(REPOSITORY_NAME, indexUUID, "0");
    }

    public void testReleaseLockOnNonExistingMetadata() {
        // validate release lock on non-existing metadata goes through fine
        try {
            remoteStoreMetadataLockManager.release(FileLockInfo.getLockInfoBuilder().withAcquirerId("acquirer-1").build());
        } catch (Exception e) {
            fail("not excepted to throw exception but got " + e);
        }
    }

    public void testAcquireLockOnNonExistingMetadata() {
        // passing invalid primary term and invalid generation
        assertThrows(NoSuchFileException.class, () -> remoteSegmentStoreDirectory.acquireLock(29, 36, "acquirer-1"));
    }

    public void testGCWithLocks() throws Exception {
        // index data
        client().prepareIndex(INDEX_NAME).setId(UUIDs.randomBase64UUID()).setSource(randomAlphaOfLength(5), randomAlphaOfLength(5)).get();
        // commit data
        flush(INDEX_NAME);

        Path mdFilePath = Path.of(String.valueOf(absolutePath), indexUUID, "0", "segments", "metadata");
        List<String> mdFiles = getFilesInPath(mdFilePath);

        String mdFile = mdFiles.get(0);
        // acquire lock on mdFile
        remoteStoreMetadataLockManager.acquire(
            FileLockInfo.getLockInfoBuilder().withFileToLock(mdFile).withAcquirerId("acquirer-1").build()
        );

        // acquiring multiple locks on same md file
        remoteStoreMetadataLockManager.acquire(
            FileLockInfo.getLockInfoBuilder().withFileToLock(mdFile).withAcquirerId("acquirer-2").build()
        );
        remoteStoreMetadataLockManager.acquire(
            FileLockInfo.getLockInfoBuilder().withFileToLock(mdFile).withAcquirerId("acquirer-3").build()
        );

        // delete stale segments, validate md file still persists
        remoteSegmentStoreDirectory.deleteStaleSegments(0);
        mdFiles = getFilesInPath(mdFilePath);
        assertTrue(mdFiles.contains(mdFile));

        // release 2 locks from md file and delete stale segments
        remoteStoreMetadataLockManager.release(FileLockInfo.getLockInfoBuilder().withAcquirerId("acquirer-1").build());
        remoteStoreMetadataLockManager.release(FileLockInfo.getLockInfoBuilder().withAcquirerId("acquirer-2").build());
        remoteSegmentStoreDirectory.deleteStaleSegments(0);

        // validate md file still exists
        mdFiles = getFilesInPath(mdFilePath);
        assertTrue(mdFiles.contains(mdFile));

        // release all locks and delete stale segments
        remoteStoreMetadataLockManager.release(FileLockInfo.getLockInfoBuilder().withAcquirerId("acquirer-3").build());
        remoteSegmentStoreDirectory.deleteStaleSegments(0);

        // validate md file gc as it don't have any locks
        mdFiles = getFilesInPath(mdFilePath);
        assertFalse(mdFiles.contains(mdFile));
    }
}
