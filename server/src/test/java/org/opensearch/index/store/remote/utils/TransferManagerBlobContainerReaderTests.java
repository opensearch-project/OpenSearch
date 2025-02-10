/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils;

import org.opensearch.common.blobstore.BlobContainer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class TransferManagerBlobContainerReaderTests extends TransferManagerTestCase {
    private BlobContainer blobContainer;

    @Override
    protected void initializeTransferManager() throws IOException {
        blobContainer = mock(BlobContainer.class);
        doAnswer(i -> new ByteArrayInputStream(createData())).when(blobContainer).readBlob(eq("blob"), anyLong(), anyLong());
        transferManager = new TransferManager(blobContainer::readBlob, fileCache);
    }

    protected void mockExceptionWhileReading() throws IOException {
        doThrow(new IOException("Expected test exception")).when(blobContainer).readBlob(eq("failure-blob"), anyLong(), anyLong());
    }

    protected void mockWaitForLatchReader(CountDownLatch latch) throws IOException {
        doAnswer(i -> {
            latch.await();
            return new ByteArrayInputStream(createData());
        }).when(blobContainer).readBlob(eq("blocking-blob"), anyLong(), anyLong());
    }
}
