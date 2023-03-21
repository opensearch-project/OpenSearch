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
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class FileBasedMDShardLockManagerTests extends OpenSearchTestCase {

    private RemoteDirectory lockDirectory;
    private FileBasedMDShardLockManager fileBasedMDShardLockManager;
    @Before
    public void setup() throws IOException {
        lockDirectory = mock(RemoteBufferedOutputDirectory.class);

        fileBasedMDShardLockManager = new FileBasedMDShardLockManager(lockDirectory);
    }

    public void testAcquire() throws IOException {

        String testLockName = "testLock";
        IndexOutput indexOutput = mock(IndexOutput.class);
        when(lockDirectory.createOutput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexOutput);

        RemoteStoreMDShardLockManager.ShardLockInfo lockInfoMock = mock(FileBasedMDShardLockManager.ShardLockFileInfo.class);
        when(lockInfoMock.getLockName()).thenReturn(testLockName);

        fileBasedMDShardLockManager.acquire(lockInfoMock);
        verify(lockInfoMock).writeLockContent(any());

    }

    public void testRelease() throws IOException {
        String testLockName = "testLock";

        fileBasedMDShardLockManager.release(testLockName);
        verify(lockDirectory).deleteFile(testLockName);
    }

    public void testReadLockData() throws IOException {
        String testLockName = "testLock";

        String testMDFileName = "testMDFile";
        String resourceId = "testResourceId";

        IndexInput indexInput = mock(IndexInput.class);
        when(lockDirectory.openInput(eq(testLockName), eq(IOContext.DEFAULT))).thenReturn(indexInput);

        Map<String, String> lockDataMock = new java.util.HashMap<>();
        lockDataMock.put(RemoteStoreLockManagerUtils.METADATA_FILE_NAME, testMDFileName);
        lockDataMock.put(RemoteStoreLockManagerUtils.RESOURCE_ID, resourceId);
        when(indexInput.readMapOfStrings()).thenReturn(lockDataMock);

        FileBasedMDShardLockManager.ShardLockFileInfo output = fileBasedMDShardLockManager.readLockData(testLockName);
        assertEquals(output.getMetadataFile(), testMDFileName);
        assertEquals(output.getResourceId(), resourceId);
    }

    public void testGetLockInfoBuilder() {
        assert(fileBasedMDShardLockManager.getLockInfoBuilder().getClass().getName().equals(
            FileBasedMDShardLockManager.ShardLockFileInfo.LockInfoBuilder.class.getName()));
    }
}
