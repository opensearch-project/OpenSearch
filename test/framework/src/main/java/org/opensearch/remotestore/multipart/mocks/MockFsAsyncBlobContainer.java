
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore.multipart.mocks;

import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MockFsAsyncBlobContainer extends FsBlobContainer implements AsyncMultiStreamBlobContainer {

    private static final int TRANSFER_TIMEOUT_MILLIS = 30000;

    private final boolean triggerDataIntegrityFailure;

    public MockFsAsyncBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path, boolean triggerDataIntegrityFailure) {
        super(blobStore, blobPath, path);
        this.triggerDataIntegrityFailure = triggerDataIntegrityFailure;
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {

        int nParts = 10;
        long partSize = writeContext.getFileSize() / nParts;
        StreamContext streamContext = writeContext.getStreamProvider(partSize);
        final Path file = path.resolve(writeContext.getFileName());
        byte[] buffer = new byte[(int) writeContext.getFileSize()];
        AtomicLong totalContentRead = new AtomicLong();
        CountDownLatch latch = new CountDownLatch(streamContext.getNumberOfParts());
        for (int partIdx = 0; partIdx < streamContext.getNumberOfParts(); partIdx++) {
            int finalPartIdx = partIdx;
            Thread thread = new Thread(() -> {
                try {
                    InputStreamContainer inputStreamContainer = streamContext.provideStream(finalPartIdx);
                    InputStream inputStream = inputStreamContainer.getInputStream();
                    long remainingContentLength = inputStreamContainer.getContentLength();
                    long offset = partSize * finalPartIdx;
                    while (remainingContentLength > 0) {
                        int readContentLength = inputStream.read(buffer, (int) offset, (int) remainingContentLength);
                        totalContentRead.addAndGet(readContentLength);
                        remainingContentLength -= readContentLength;
                        offset += readContentLength;
                    }
                    inputStream.close();
                } catch (IOException e) {
                    completionListener.onFailure(e);
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        try {
            if (!latch.await(TRANSFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                throw new IOException("Timed out waiting for file transfer to complete for " + writeContext.getFileName());
            }
        } catch (InterruptedException e) {
            throw new IOException("Await interrupted on CountDownLatch, transfer failed for " + writeContext.getFileName());
        }
        try (OutputStream outputStream = Files.newOutputStream(file, StandardOpenOption.CREATE_NEW)) {
            outputStream.write(buffer);
        }
        if (writeContext.getFileSize() != totalContentRead.get()) {
            throw new IOException(
                "Incorrect content length read for file "
                    + writeContext.getFileName()
                    + ", actual file size: "
                    + writeContext.getFileSize()
                    + ", bytes read: "
                    + totalContentRead.get()
            );
        }

        try {
            // bulks need to succeed for segment files to be generated
            if (isSegmentFile(writeContext.getFileName()) && triggerDataIntegrityFailure) {
                completionListener.onFailure(
                    new RuntimeException(
                        new CorruptIndexException(
                            "Data integrity check failure for file: " + writeContext.getFileName(),
                            writeContext.getFileName()
                        )
                    )
                );
            } else {
                writeContext.getUploadFinalizer().accept(true);
                completionListener.onResponse(null);
            }
        } catch (Exception e) {
            completionListener.onFailure(e);
        }

    }

    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        new Thread(() -> {
            try {
                long contentLength = listBlobs().get(blobName).length();
                long partSize = contentLength / 10;
                int numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
                List<ReadContext.StreamPartCreator> blobPartStreams = new ArrayList<>();
                for (int partNumber = 0; partNumber < numberOfParts; partNumber++) {
                    long offset = partNumber * partSize;
                    InputStreamContainer blobPartStream = new InputStreamContainer(readBlob(blobName, offset, partSize), partSize, offset);
                    blobPartStreams.add(() -> CompletableFuture.completedFuture(blobPartStream));
                }
                ReadContext blobReadContext = new ReadContext.Builder(contentLength, blobPartStreams).build();
                listener.onResponse(blobReadContext);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }).start();
    }

    public boolean remoteIntegrityCheckSupported() {
        return true;
    }

    private boolean isSegmentFile(String filename) {
        return !filename.endsWith(".tlog") && !filename.endsWith(".ckp");
    }

    @Override
    public void deleteAsync(ActionListener<DeleteResult> completionListener) {
        throw new UnsupportedOperationException("deleteAsync");
    }

    @Override
    public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> completionListener) {
        throw new UnsupportedOperationException("deleteBlobsAsyncIgnoringIfNotExists");
    }
}
