/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import junit.framework.TestCase;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteStoreMetadataLockManagerTests extends OpenSearchTestCase {
    private RemoteBufferedOutputDirectory lockDirectory;
    private RemoteStoreMetadataLockManager remoteStoreMetadataLockManager;
    String testLockName = "testLock";
    String testMetadata = "testMetadata";
    String testAcquirerId = "testAcquirerId";

    @Before
    public void setup() throws IOException {
        lockDirectory = mock(RemoteBufferedOutputDirectory.class);

        remoteStoreMetadataLockManager = new RemoteStoreMetadataLockManager(lockDirectory);
    }

    private Collection<String> getListOfLocksMock() {
        return Arrays.asList(
            String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, testAcquirerId)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION,
            String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, "acquirerId2")
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION
        );
    }

    public void testAcquire() throws IOException {
        IndexOutput indexOutput = mock(IndexOutput.class);
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).withAcquirerId(testAcquirerId).build();
        when(lockDirectory.createOutput(eq(testLockInfo.generateLockName()), eq(IOContext.DEFAULT))).thenReturn(indexOutput);
        remoteStoreMetadataLockManager.acquire(testLockInfo);
        verify(indexOutput).close();
    }

    public void testAcquireOnlyFileToLockPassed() { // only fileToLock was passed to acquire call.
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(lockDirectory.createOutput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexOutput);
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).build();
        assertThrows(IllegalArgumentException.class, () -> remoteStoreMetadataLockManager.acquire(testLockInfo));
    }

    public void testAcquireOnlyAcquirerIdPassed() { // only AcquirerId was passed to acquire call.
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(lockDirectory.createOutput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexOutput);
        LockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, () -> remoteStoreMetadataLockManager.acquire(testLockInfo));
    }

    public void testRelease() throws IOException {
        when(lockDirectory.listAll()).thenReturn(getListOfLocksMock().toArray(new String[0]));
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();

        remoteStoreMetadataLockManager.release(testLockInfo);
        verify(lockDirectory).deleteFile(
            String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, testAcquirerId)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION
        );
    }

    public void testReleaseExceptionCase() { // acquirerId is Not passed during release lock call.
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).build();
        assertThrows(IllegalArgumentException.class, () -> remoteStoreMetadataLockManager.release(testLockInfo));
    }

    public void testIsAcquired() throws IOException {
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withFileToLock(testMetadata).build();
        when(lockDirectory.listFilesByPrefix(testLockInfo.getLockPrefix())).thenReturn(getListOfLocksMock());
        TestCase.assertTrue(remoteStoreMetadataLockManager.isAcquired(testLockInfo));
    }

    public void testIsAcquiredExceptionCase() { // metadata file is not passed during isAcquired call.
        FileLockInfo testLockInfo = FileLockInfo.getLockInfoBuilder().withAcquirerId(testAcquirerId).build();
        assertThrows(IllegalArgumentException.class, () -> remoteStoreMetadataLockManager.isAcquired(testLockInfo));
    }

    public void testFetchLocksEmpty() throws IOException {
        when(lockDirectory.listFilesByPrefix("metadata")).thenReturn(Set.of());
        assertEquals(0, remoteStoreMetadataLockManager.fetchLockedMetadataFiles("metadata").size());
    }

    public void testFetchLocksNonEmpty() throws IOException {
        String metadata1 = "metadata_1_2_3";
        String metadata2 = "metadata_4_5_6";
        when(lockDirectory.listFilesByPrefix("metadata")).thenReturn(
            Set.of(
                FileLockInfo.LockFileUtils.generateLockName(metadata1, "snapshot1"),
                FileLockInfo.LockFileUtils.generateLockName(metadata2, "snapshot2")
            )
        );
        assertEquals(Set.of(metadata1, metadata2), remoteStoreMetadataLockManager.fetchLockedMetadataFiles("metadata"));
    }

    public void testFetchLocksException() throws IOException {
        when(lockDirectory.listFilesByPrefix("metadata")).thenThrow(new IOException("Something went wrong"));
        assertThrows(IOException.class, () -> remoteStoreMetadataLockManager.fetchLockedMetadataFiles("metadata"));
    }
}
