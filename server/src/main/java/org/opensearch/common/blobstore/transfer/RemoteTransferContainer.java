/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import com.jcraft.jzlib.JZlib;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.common.OffsetStreamContainer;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StreamContext;
import org.opensearch.common.ThrowingTriFunction;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.ResettableCheckedInputStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Objects;

/**
 * RemoteTransferContainer is an encapsulation for managing transfers for translog and segment files.
 */
public class RemoteTransferContainer implements Closeable {

    private int numberOfParts;
    private long partSize;
    private long lastPartSize;

    private final long contentLength;
    private final SetOnce<ResettableCheckedInputStream[]> inputStreams = new SetOnce<>();
    private final String localFileName;
    private final String remoteFileName;
    private final boolean failTransferIfFileExists;
    private final WritePriority writePriority;
    private final long expectedChecksum;
    private final OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier;

    private static final Logger log = LogManager.getLogger(RemoteTransferContainer.class);

    /**
     * Construct a new RemoteTransferContainer object using {@link Path} reference to the file.
     * This constructor calculates the <code>expectedChecksum</code> of the uploaded file internally by calling
     * <code>TranslogCheckedContainer#getChecksum</code>
     *
     * @param localFileName                  Name of the local file
     * @param remoteFileName                 Name of the remote file
     * @param contentLength                  Total content length of the file to be uploaded
     * @param failTransferIfFileExists       A boolean to determine if upload has to be failed if file exists
     * @param writePriority                  The {@link WritePriority} of current upload
     * @param offsetRangeInputStreamSupplier A supplier to create OffsetRangeInputStreams
     */
    public RemoteTransferContainer(
        String localFileName,
        String remoteFileName,
        long contentLength,
        boolean failTransferIfFileExists,
        WritePriority writePriority,
        OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier,
        long expectedChecksum
    ) {
        this.localFileName = localFileName;
        this.remoteFileName = remoteFileName;
        this.contentLength = contentLength;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.writePriority = writePriority;
        this.offsetRangeInputStreamSupplier = offsetRangeInputStreamSupplier;
        this.expectedChecksum = expectedChecksum;
    }

    /**
     * @return The {@link  WriteContext} for the current upload
     */
    public WriteContext createWriteContext() {
        return new WriteContext(
            remoteFileName,
            this::supplyStreamContext,
            contentLength,
            failTransferIfFileExists,
            writePriority,
            this::finalizeUpload
        );
    }

    /**
     * @param partSize Part sizes of all parts apart from the last one, which is determined internally
     * @return The {@link StreamContext} object that will be used by the vendor plugin to retrieve streams during upload
     */
    public StreamContext supplyStreamContext(long partSize) {
        try {
            return this.openMultipartStreams(partSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private StreamContext openMultipartStreams(long partSize) throws IOException {
        if (inputStreams.get() != null) {
            throw new IOException("Multi-part streams are already created.");
        }

        this.partSize = partSize;
        this.lastPartSize = (contentLength % partSize) != 0 ? contentLength % partSize : partSize;
        this.numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
        ResettableCheckedInputStream[] streams = new ResettableCheckedInputStream[numberOfParts];
        inputStreams.set(streams);

        return new StreamContext(getTransferPartStreamSupplier(), partSize, lastPartSize, numberOfParts);
    }

    private ThrowingTriFunction<Integer, Long, Long, OffsetStreamContainer, IOException> getTransferPartStreamSupplier() {
        return ((partNo, size, position) -> {
            assert inputStreams.get() != null : "expected inputStreams to be initialised";
            return getMultipartStreamSupplier(partNo, size, position).get();
        });
    }

    /**
     * OffsetRangeInputStreamSupplier is used to get the offset based input streams at runtime
     */
    public interface OffsetRangeInputStreamSupplier {
        OffsetRangeInputStream get(long size, long position) throws IOException;
    }

    interface LocalStreamSupplier<Stream> {
        Stream get() throws IOException;
    }

    private LocalStreamSupplier<OffsetStreamContainer> getMultipartStreamSupplier(
        final int streamIdx,
        final long size,
        final long position
    ) {
        return () -> {
            try {
                OffsetRangeInputStream offsetRangeInputStream = offsetRangeInputStreamSupplier.get(size, position);
                ResettableCheckedInputStream checkedInputStream = new ResettableCheckedInputStream(offsetRangeInputStream, localFileName);
                Objects.requireNonNull(inputStreams.get())[streamIdx] = checkedInputStream;

                return new OffsetStreamContainer(checkedInputStream, size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                throw e;
            }
        };
    }

    private void finalizeUpload(boolean uploadSuccessful) {
        if (uploadSuccessful) {
            long actualChecksum = getActualChecksum();
            if (actualChecksum != expectedChecksum) {
                throw new RuntimeException(
                    new CorruptIndexException(
                        "Data integrity check done after upload for file "
                            + localFileName
                            + " failed, actual checksum: "
                            + actualChecksum
                            + ", expected checksum: "
                            + expectedChecksum,
                        localFileName
                    )
                );
            }
        }
    }

    /**
     * @return The total content length of current upload
     */
    public long getContentLength() {
        return contentLength;
    }

    private long getActualChecksum() {
        long checksum = Objects.requireNonNull(inputStreams.get())[0].getChecksum();
        for (int checkSumIdx = 1; checkSumIdx < Objects.requireNonNull(inputStreams.get()).length - 1; checkSumIdx++) {
            checksum = JZlib.crc32_combine(checksum, Objects.requireNonNull(inputStreams.get())[checkSumIdx].getChecksum(), partSize);
        }
        if (numberOfParts > 1) {
            checksum = JZlib.crc32_combine(
                checksum,
                Objects.requireNonNull(inputStreams.get())[numberOfParts - 1].getChecksum(),
                lastPartSize
            );
        }

        return checksum;
    }

    @Override
    public void close() throws IOException {
        if (inputStreams.get() == null) {
            log.warn("Input streams cannot be closed since they are not yet set for multi stream upload");
            return;
        }

        boolean closeStreamException = false;
        for (InputStream is : Objects.requireNonNull(inputStreams.get())) {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException ex) {
                closeStreamException = true;
                // Attempting to close all streams first before throwing exception.
                log.error("Multipart stream failed to close ", ex);
            }
        }

        if (closeStreamException) {
            throw new IOException("Closure of some of the multi-part streams failed.");
        }
    }
}
