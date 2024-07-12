/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.apache.lucene.store.IOContext;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.InputStreamIndexInput;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TransferManagerRemoteDirectoryReaderTests extends TransferManagerTestCase {
    private RemoteDirectory remoteDirectory;

    @Override
    protected void initializeTransferManager() throws IOException {
        remoteDirectory = mock(RemoteDirectory.class);
        doAnswer(i -> new ByteArrayIndexInput("blob", createData())).when(remoteDirectory).openInput(eq("blob"), any());
        transferManager = new TransferManager(
            (name, position, length) -> new InputStreamIndexInput(
                remoteDirectory.openInput(name, new BlockIOContext(IOContext.DEFAULT, position, length)),
                length
            ),
            fileCache
        );
    }

    protected void mockExceptionWhileReading() throws IOException {
        doThrow(new IOException("Expected test exception")).when(remoteDirectory).openInput(eq("failure-blob"), any());
    }

    protected void mockWaitForLatchReader(CountDownLatch latch) throws IOException {
        doAnswer(i -> {
            latch.await();
            return new ByteArrayIndexInput("blocking-blob", createData());
        }).when(remoteDirectory).openInput(eq("blocking-blob"), any());
    }
}
