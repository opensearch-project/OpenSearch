/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.blobstore.stream.listener.FileCompletionListener;
import org.opensearch.common.blobstore.stream.listener.StreamCompletionListener;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An extension of {@link BlobContainer} that adds {@link VerifyingMultiStreamBlobContainer#asyncBlobUpload} to allow
 * multipart uploads and performs integrity checks on transferred files
 *
 * @opensearch.internal
 */
public interface VerifyingMultiStreamBlobContainer extends BlobContainer {

    /**
     * Reads blob content from multiple streams, each from a specific part of the file, which is provided by the
     * StreamContextSupplier in the WriteContext passed to this method. An {@link IOException} is thrown if reading
     * any of the input streams fails, or writing to the target blob fails
     *
     * @param writeContext         A WriteContext object encapsulating all information needed to perform the upload
     * @param completionListener   Listener on which upload events should be published.
     * @throws IOException if any of the input streams could not be read, or the target blob could not be written to
     */
    void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException;

    /**
     * Creates an async callback of an {@link java.io.InputStream} for the specified blob within the container.
     * An {@link IOException} is thrown if requesting the input stream fails.
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param position The position in the blob where the next byte will be read.
     * @param length   An indication of the number of bytes to be read.
     * @param listener  Async listener for {@link InputStream} object which serves the input streams and other metadata for the blob
     */
    void readBlobAsync(String blobName, long position, long length, ActionListener<InputStream> listener);

    /**
     * Fetches the checksum for the blob stored within the repository implementation
     * @param blobName The name of the blob for which repository checksum is needed
     * @return checksum string of the blob
     * @throws IOException in case of any issues with the blob
     */
    default String blobChecksum(String blobName) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Downloads the blob to the given path location using an async mechanism
     * @param blobName The name of the blob which is to be fetched from the container
     * @param segmentFileLocation The location where the blob needs to be downloade
     * @param segmentCompletionListener Async listener for notification of completion or failure
     */
    default void asyncBlobDownload(String blobName, Path segmentFileLocation, ActionListener<String> segmentCompletionListener) {
        try {
            final long segmentSize = listBlobs().get(blobName).length();
            final long optimalStreamSize = readBlobPreferredLength();
            final int numStreams = (int) Math.ceil(segmentSize * 1.0 / optimalStreamSize);

            final AtomicBoolean anyStreamFailed = new AtomicBoolean();
            final List<String> partFileNames = Collections.synchronizedList(new ArrayList<>());

            final String segmentFileName = segmentFileLocation.getFileName().toString();
            final Path segmentDirectory = segmentFileLocation.getParent();

            final FileCompletionListener fileCompletionListener = new FileCompletionListener(
                numStreams,
                segmentFileName,
                segmentDirectory,
                partFileNames,
                anyStreamFailed,
                segmentCompletionListener
            );

            for (int streamNumber = 0; streamNumber < numStreams; streamNumber++) {
                String partFileName = UUID.randomUUID().toString();
                long start = streamNumber * optimalStreamSize;
                long end = Math.min(segmentSize, ((streamNumber + 1) * optimalStreamSize));
                long length = end - start;
                partFileNames.add(partFileName);

                final StreamCompletionListener streamCompletionListener = new StreamCompletionListener(
                    partFileName,
                    segmentDirectory,
                    anyStreamFailed,
                    fileCompletionListener
                );
                readBlobAsync(blobName, start, length, streamCompletionListener);
            }
        } catch (Exception e) {
            segmentCompletionListener.onFailure(e);
        }

    }
}
