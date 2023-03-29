/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SetOnce;
import org.opensearch.common.Stream;
import org.opensearch.common.StreamIterable;
import org.opensearch.common.TransferPartStreamSupplier;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.index.translog.ChannelFactory;
import org.opensearch.index.translog.checked.TranslogCheckedContainer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.function.Supplier;

public class RemoteTransferContainer implements Closeable {

    private Path localFile;
    private Directory directory;
    private IOContext ioContext;
    private int numberOfParts;
    private long partSize;
    private long lastPartSize;

    private final long contentLength;
    private final SetOnce<InputStream[]> inputStreams = new SetOnce<>();
    private final String localFileName;
    private final String remoteFileName;
    private final boolean failTransferIfFileExists;
    private final WritePriority writePriority;
    private final long expectedChecksum;

    private static final Logger log = LogManager.getLogger(RemoteTransferContainer.class);

    // calculates file checksum internally
    public RemoteTransferContainer(Path localFile,
                                   String localFileName,
                                   String remoteFileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority) throws IOException {
        this.localFileName = localFileName;
        this.remoteFileName = remoteFileName;
        this.localFile = localFile;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.writePriority = writePriority;

        ChannelFactory channelFactory = FileChannel::open;
        localFile.getFileSystem().provider();
        try (FileChannel channel = channelFactory.open(localFile, StandardOpenOption.READ)) {
            this.contentLength = channel.size();
            TranslogCheckedContainer translogCheckedContainer = new TranslogCheckedContainer(channel, 0, (int) contentLength, localFileName);
            this.expectedChecksum = translogCheckedContainer.getChecksum();
        }
    }

    public RemoteTransferContainer(Directory directory,
                                   IOContext ioContext,
                                   String localFileName,
                                   String remoteFileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority,
                                   long expectedChecksum) throws IOException {
        this.localFileName = localFileName;
        this.remoteFileName = remoteFileName;
        this.directory = directory;
        this.expectedChecksum = expectedChecksum;
        this.failTransferIfFileExists = failTransferIfFileExists;
        try(IndexInput indexInput = directory.openInput(this.localFileName, ioContext)) {
            this.contentLength = indexInput.length();
        }
        this.ioContext = ioContext;
        this.writePriority = writePriority;
    }

    public WriteContext createWriteContext() {
        return new WriteContext(
            remoteFileName,
            this::supplyStreamContext,
            contentLength,
            failTransferIfFileExists,
            writePriority,
            expectedChecksum
        );
    }

    public StreamContext supplyStreamContext(long partSize) {
        try {
            return this.openMultipartStreams(partSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public StreamContext openMultipartStreams(long partSize) throws IOException {
        if (inputStreams.get() != null) {
            throw new IOException("Multi-part streams are already created.");
        }

        this.partSize = partSize;
        this.lastPartSize = (contentLength % partSize) != 0 ? contentLength % partSize : partSize;
        this.numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize
            : (contentLength / partSize) + 1);

        InputStream[] streams = new InputStream[numberOfParts];
        inputStreams.set(streams);

        return new StreamContext(
            new StreamIterable(getTransferPartStreamSupplier(), partSize, lastPartSize, numberOfParts),
            contentLength,
            numberOfParts
        );
    }

    private TransferPartStreamSupplier getTransferPartStreamSupplier() {
        return ((partNo, size, position) -> {
            if (localFile != null) {
                return getMultiPartStreamSupplierForFile(partNo, size, position).get();
            } else {
                return getMultiPartStreamSupplierForIndexInput(partNo, size, position).get();
            }
        });
    }

    private Supplier<Stream> getMultiPartStreamSupplierForFile(final int streamIdx, final long size,
                                                               final long position) {
        return () -> {
            OffsetRangeFileInputStream offsetRangeInputStream;
            try {
                if (inputStreams.get() == null) {
                    throw new IllegalArgumentException("InputStream parts not yet defined.");
                }
                offsetRangeInputStream = new OffsetRangeFileInputStream(localFile, size, position);
                Objects.requireNonNull(inputStreams.get())[streamIdx] = offsetRangeInputStream;
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            return new Stream(offsetRangeInputStream, size, position);
        };
    }

    private Supplier<Stream> getMultiPartStreamSupplierForIndexInput(final int streamIdx, final long size,
                                                                     final long position) {
        return () -> {
            OffsetRangeIndexInputStream offsetRangeInputStream;
            try {
                if (inputStreams.get() == null) {
                    throw new IllegalArgumentException("InputStream parts not yet defined.");
                }
                IndexInput indexInput = directory.openInput(localFileName, ioContext);
                offsetRangeInputStream = new OffsetRangeIndexInputStream(indexInput, size, position);
                Objects.requireNonNull(inputStreams.get())[streamIdx] = offsetRangeInputStream;
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            return new Stream(offsetRangeInputStream, size, position);
        };
    }

    public long getContentLength() {
        return contentLength;
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
