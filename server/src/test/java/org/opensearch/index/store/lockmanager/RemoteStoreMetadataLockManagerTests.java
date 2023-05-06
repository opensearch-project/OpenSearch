/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import junit.framework.TestCase;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.opensearch.index.store.RemoteBufferedOutputDirectory;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

public class RemoteStoreMetadataLockManagerTests extends OpenSearchTestCase {
    private RemoteDirectory lockDirectory;
    private RemoteStoreMetadataLockManager remoteStoreMetadataLockManager;
    String testLockName = "testLock";
    String testMetadata = "testMetadata";
    String testAcquirerId = "testAcquirerId";

    @Before
    public void setup() throws IOException {
        lockDirectory = mock(RemoteBufferedOutputDirectory.class);

        remoteStoreMetadataLockManager = new RemoteStoreMetadataLockManager(lockDirectory);
    }

    private FileLockInfo getFileLockInfoMock() {
        FileLockInfo lockInfoMock = mock(FileLockInfo.class);
        when(lockInfoMock.getFileToLock()).thenReturn(testMetadata);
        when(lockInfoMock.getAcquirerId()).thenReturn(testAcquirerId);
        return lockInfoMock;
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
        when(lockDirectory.createOutput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexOutput);
        LockInfo lockInfoMock = getFileLockInfoMock();
        when(lockInfoMock.generateLockName()).thenReturn(testLockName);
        remoteStoreMetadataLockManager.acquire(lockInfoMock);
        verify(indexOutput).close();
    }

    public void testRelease() throws IOException {
        FileLockInfo lockInfoMock = getFileLockInfoMock();
        when(lockDirectory.listAll()).thenReturn(getListOfLocksMock().toArray(new String[0]));
        when(lockInfoMock.getLocksForAcquirer(any())).thenReturn(
            List.of(
                String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, testAcquirerId)
                    + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION
            )
        );

        remoteStoreMetadataLockManager.release(lockInfoMock);
        verify(lockDirectory).deleteFile(
            String.join(RemoteStoreLockManagerUtils.SEPARATOR, testMetadata, testAcquirerId)
                + RemoteStoreLockManagerUtils.LOCK_FILE_EXTENSION
        );
    }

    public void testIsAcquired() throws IOException {
        FileLockInfo lockInfoMock = mock(FileLockInfo.class);
        when(lockInfoMock.getFileToLock()).thenReturn(testMetadata);
        when(lockInfoMock.getLockPrefix()).thenReturn(testMetadata + RemoteStoreLockManagerUtils.SEPARATOR);
        when(lockDirectory.listFilesByPrefix(testMetadata + RemoteStoreLockManagerUtils.SEPARATOR)).thenReturn(getListOfLocksMock());
        TestCase.assertTrue(remoteStoreMetadataLockManager.isAcquired(lockInfoMock));
    }
}
