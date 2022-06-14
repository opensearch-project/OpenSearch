/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doThrow;

public class RemoteStoreRefreshListenerTests extends OpenSearchTestCase {
    private Directory storeDirectory;
    private Directory remoteDirectory;

    private RemoteStoreRefreshListener remoteStoreRefreshListener;

    public void setup(String[] remoteFiles) throws IOException {
        storeDirectory = mock(Directory.class);
        remoteDirectory = mock(Directory.class);
        when(remoteDirectory.listAll()).thenReturn(remoteFiles);
        remoteStoreRefreshListener = new RemoteStoreRefreshListener(storeDirectory, remoteDirectory);
    }

    public void testAfterRefreshFalse() throws IOException {
        setup(new String[0]);
        remoteStoreRefreshListener.afterRefresh(false);
        verify(storeDirectory, times(0)).listAll();
    }

    public void testAfterRefreshTrueNoLocalFiles() throws IOException {
        setup(new String[0]);

        when(storeDirectory.listAll()).thenReturn(new String[0]);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory, times(0)).deleteFile(any());
    }

    public void testAfterRefreshOnlyUploadFiles() throws IOException {
        setup(new String[0]);

        String[] localFiles = new String[] { "segments_1", "0.si", "0.cfs", "0.cfe" };
        when(storeDirectory.listAll()).thenReturn(localFiles);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory).copyFrom(storeDirectory, "segments_1", "segments_1", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.si", "0.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfs", "0.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfe", "0.cfe", IOContext.DEFAULT);
        verify(remoteDirectory, times(0)).deleteFile(any());
    }

    public void testAfterRefreshOnlyUploadAndDelete() throws IOException {
        setup(new String[] { "0.si", "0.cfs" });

        String[] localFiles = new String[] { "segments_1", "1.si", "1.cfs", "1.cfe" };
        when(storeDirectory.listAll()).thenReturn(localFiles);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory).copyFrom(storeDirectory, "segments_1", "segments_1", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.si", "1.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfs", "1.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfe", "1.cfe", IOContext.DEFAULT);
        verify(remoteDirectory).deleteFile("0.si");
        verify(remoteDirectory).deleteFile("0.cfs");
    }

    public void testAfterRefreshOnlyDelete() throws IOException {
        setup(new String[] { "0.si", "0.cfs" });

        String[] localFiles = new String[] { "0.si" };
        when(storeDirectory.listAll()).thenReturn(localFiles);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory, times(0)).copyFrom(any(), any(), any(), any());
        verify(remoteDirectory).deleteFile("0.cfs");
    }

    public void testAfterRefreshTempLocalFile() throws IOException {
        setup(new String[0]);

        String[] localFiles = new String[] { "segments_1", "0.si", "0.cfs.tmp" };
        when(storeDirectory.listAll()).thenReturn(localFiles);
        doThrow(new NoSuchFileException("0.cfs.tmp")).when(remoteDirectory)
            .copyFrom(storeDirectory, "0.cfs.tmp", "0.cfs.tmp", IOContext.DEFAULT);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory).copyFrom(storeDirectory, "segments_1", "segments_1", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.si", "0.si", IOContext.DEFAULT);
        verify(remoteDirectory, times(0)).deleteFile(any());
    }

    public void testAfterRefreshConsecutive() throws IOException {
        setup(new String[0]);

        String[] localFiles = new String[] { "segments_1", "0.si", "0.cfs", "0.cfe" };
        when(storeDirectory.listAll()).thenReturn(localFiles);
        doThrow(new IOException("0.cfs")).when(remoteDirectory).copyFrom(storeDirectory, "0.cfs", "0.cfe", IOContext.DEFAULT);
        doThrow(new IOException("0.cfe")).when(remoteDirectory).copyFrom(storeDirectory, "0.cfe", "0.cfe", IOContext.DEFAULT);

        remoteStoreRefreshListener.afterRefresh(true);
        verify(storeDirectory).listAll();
        verify(remoteDirectory).copyFrom(storeDirectory, "segments_1", "segments_1", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.si", "0.si", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfs", "0.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfe", "0.cfe", IOContext.DEFAULT);
        verify(remoteDirectory, times(0)).deleteFile(any());

        String[] localFilesSecondRefresh = new String[] { "segments_1", "0.cfs", "1.cfs", "1.cfe" };
        when(storeDirectory.listAll()).thenReturn(localFilesSecondRefresh);

        remoteStoreRefreshListener.afterRefresh(true);

        verify(remoteDirectory).copyFrom(storeDirectory, "0.cfs", "0.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfs", "1.cfs", IOContext.DEFAULT);
        verify(remoteDirectory).copyFrom(storeDirectory, "1.cfe", "1.cfe", IOContext.DEFAULT);
        verify(remoteDirectory).deleteFile("0.si");
    }
}
