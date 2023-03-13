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
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.SetOnce;
import org.opensearch.common.Stream;
import org.opensearch.common.blobstore.stream.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.index.translog.BufferedChecksumStreamInput;
import org.opensearch.index.translog.ChannelFactory;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

public class RemoteTransferContainer implements Closeable {


    private Path localFile;
    private IndexInput indexInput;
    private int numberOfParts;
    private long partSize;
    private long lastPartSize;

    private final long contentLength;
    private final SetOnce<BufferedInputStream[]> inputStreams = new SetOnce<>();
    private final String fileName;
    private final boolean failTransferIfFileExists;
    private final WritePriority writePriority;

    private static final Logger log = LogManager.getLogger(RemoteTransferContainer.class);

    public RemoteTransferContainer(Path localFile,
                                   String fileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority) throws IOException {
        this.fileName = fileName;
        this.localFile = localFile;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.writePriority = writePriority;

        ChannelFactory channelFactory = FileChannel::open;
        localFile.getFileSystem().provider();
        try (FileChannel channel = channelFactory.open(localFile, StandardOpenOption.READ)) {
            this.contentLength = channel.size();
        }
    }

    public RemoteTransferContainer(IndexInput indexInput,
                                   String fileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority) {
        this.fileName = fileName;
        this.indexInput = indexInput;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.contentLength = indexInput.length();
        this.writePriority = writePriority;
    }

    public WriteContext createWriteContext() {
        return new WriteContext(
            fileName,
            this::supplyStreamContext,
            contentLength,
            failTransferIfFileExists,
            writePriority,
            this::finalizeUpload
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

        BufferedInputStream[] checkedInputStreams = new BufferedInputStream[numberOfParts];
        List<Supplier<Stream>> streamSuppliers = new ArrayList<>();
        for (int partNo = 0; partNo < numberOfParts; partNo++) {
            long position = partSize * partNo;
            long size = partNo == numberOfParts - 1 ? lastPartSize : partSize;
            checkedInputStreams[partNo] = localFile != null
                ? getMultiPartStreamSupplierForFile().apply(size, position)
                : getMultiPartStreamSupplierForIndexInput().apply(size, position);
            if (checkedInputStreams[partNo] == null) {
                throw new IOException("Error creating multipart stream during opening streams for read");
            }

            final int finalPartNo = partNo;
            streamSuppliers.add(() -> new Stream(checkedInputStreams[finalPartNo], size, position));
        }
        inputStreams.set(checkedInputStreams);

        return new StreamContext(
            streamSuppliers,
            contentLength
        );
    }

    private BiFunction<Long, Long, BufferedInputStream> getMultiPartStreamSupplierForFile() {
        return (size, position) -> {
            OffsetRangeFileInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeFileInputStream(localFile.toFile(), size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            CheckedInputStream checkedInputStream = new CheckedInputStream(offsetRangeInputStream, new CRC32());
            return new BufferedInputStream(checkedInputStream);
        };
    }

    private BiFunction<Long, Long, BufferedInputStream> getMultiPartStreamSupplierForIndexInput() {
        return (size, position) -> {
            OffsetRangeIndexInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeIndexInputStream(indexInput, fileName, size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            CheckedInputStream checkedInputStream = new CheckedInputStream(offsetRangeInputStream, new CRC32());
            return new BufferedInputStream(checkedInputStream);
        };
    }

    public long getContentLength() {
        return contentLength;
    }

    public void finalizeUpload(boolean uploadSuccessful) {
        // verification of upload and other cleanup can be done here
    }

    @Override
    public void close() throws IOException {
        assert inputStreams.get() != null : "Input streams are not yet set for multi stream upload";

        boolean closeStreamException = false;
        for (BufferedInputStream cis : inputStreams.get()) {
            try {
                cis.close();
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
