/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.lockmanager;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.junit.Before;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;

public class FileBasedMDIndexLockManagerTests extends OpenSearchTestCase {

    private RemoteDirectory lockDirectory;
    private FileBasedMDIndexLockManager fileBasedMDIndexLockManager;
    @Before
    public void setup() throws IOException {
        lockDirectory = mock(RemoteBufferedOutputDirectory.class);

        fileBasedMDIndexLockManager = new FileBasedMDIndexLockManager(lockDirectory);
    }

    public void testAcquire() throws IOException {

        String testLockName = "testLock";
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(lockDirectory.createOutput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        RemoteStoreMDIndexLockManager.IndexLockInfo lockInfoMock = mock(FileBasedMDIndexLockManager
            .IndexLockFileInfo.class);
        when(lockInfoMock.getLockName()).thenReturn(testLockName);

        fileBasedMDIndexLockManager.acquire(lockInfoMock);
        verify(lockInfoMock).writeLockContent(any());

    }

    public void testRelease() throws IOException {
        String testLockName = "testLock";

        fileBasedMDIndexLockManager.release(testLockName);
        verify(lockDirectory).deleteFile(testLockName);
    }

    public void testReadLockData() throws IOException {
        String testLockName = "testLock";

        String testExpiryTime = Instant.now().toString();
        String resourceId = "testResourceId";

        IndexInput indexInput = mock(IndexInput.class);
        when(lockDirectory.openInput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexInput);

        Map<String, String> lockDataMock = new java.util.HashMap<>();
        lockDataMock.put(RemoteStoreLockManagerUtils.LOCK_EXPIRY_TIME, testExpiryTime);
        lockDataMock.put(RemoteStoreLockManagerUtils.RESOURCE_ID, resourceId);
        when(indexInput.readMapOfStrings()).thenReturn(lockDataMock);

        FileBasedMDIndexLockManager.IndexLockFileInfo output = fileBasedMDIndexLockManager.readLockData(testLockName);
        assertEquals(output.getResourceId(), resourceId);
        assertEquals(output.getExpiryTime(), testExpiryTime);
    }

    public void testGetLockInfoBuilder() {
        assert(fileBasedMDIndexLockManager.getLockInfoBuilder().getClass().getName().equals(
            FileBasedMDIndexLockManager.IndexLockFileInfo.LockInfoBuilder.class.getName()));
    }
}
