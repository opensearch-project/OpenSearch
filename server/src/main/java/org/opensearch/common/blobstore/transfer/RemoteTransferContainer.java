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
import org.opensearch.common.blobstore.transfer.exception.CorruptedLocalFileException;
import org.opensearch.index.translog.ChannelFactory;

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
    private final long expectedChecksum;
    private final SetOnce<CheckedInputStream[]> inputStreams = new SetOnce<>();
    private final String fileName;
    private final boolean failTransferIfFileExists;
    private final WritePriority writePriority;

    private static final Logger log = LogManager.getLogger(RemoteTransferContainer.class);

    public RemoteTransferContainer(Path localFile,
                                   long expectedChecksum,
                                   String fileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority) throws IOException {
        this.fileName = fileName;
        this.localFile = localFile;
        this.expectedChecksum = expectedChecksum;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.writePriority = writePriority;

        ChannelFactory channelFactory = FileChannel::open;
        localFile.getFileSystem().provider();
        try (FileChannel channel = channelFactory.open(localFile, StandardOpenOption.READ)) {
            this.contentLength = channel.size();
        }
    }

    public RemoteTransferContainer(IndexInput indexInput,
                                   long expectedChecksum,
                                   String fileName,
                                   boolean failTransferIfFileExists,
                                   WritePriority writePriority) {
        this.fileName = fileName;
        this.indexInput = indexInput;
        this.expectedChecksum = expectedChecksum;
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

        if (numberOfParts > 1) {
            log.info("part count is >1");
        }
        CheckedInputStream[] checkedInputStreams = new CheckedInputStream[numberOfParts];
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

    private BiFunction<Long, Long, CheckedInputStream> getMultiPartStreamSupplierForFile() {
        return (size, position) -> {
            OffsetRangeFileInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeFileInputStream(localFile.toFile(), size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            return new CheckedInputStream(offsetRangeInputStream, new CRC32());
        };
    }

    private BiFunction<Long, Long, CheckedInputStream> getMultiPartStreamSupplierForIndexInput() {
        return (size, position) -> {
            OffsetRangeIndexInputStream offsetRangeInputStream;
            try {
                offsetRangeInputStream = new OffsetRangeIndexInputStream(indexInput, fileName, size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                return null;
            }
            return new CheckedInputStream(offsetRangeInputStream, new CRC32());
        };
    }

    public long getContentLength() {
        return contentLength;
    }

    public void finalizeUpload(boolean uploadSuccessful) {
        try {
            this.close();
        } catch (IOException e) {
            log.error("Error closing RemoteTransferContainer", e);
        }
        if (uploadSuccessful) {
            try {
                this.verifyChecksum();
            } catch (CorruptedLocalFileException e) {
                log.error(String.format("Local file %s is corrupted", fileName));
            }
        }
    }

    public void verifyChecksum() throws CorruptedLocalFileException {
        int lastPartNumber = numberOfParts - 1;
        log.debug("File " + fileName + " numberOfParts: " + numberOfParts + " partSize: " + partSize +
            " rawLastPartSize: " + lastPartSize + " lastPartNumber: " + lastPartNumber);
        long checksum = inputStreams.get()[0].getChecksum().getValue();
        for (int checkSumIdx = 1; checkSumIdx < inputStreams.get().length - 1; checkSumIdx++) {
            checksum = ChecksumUtils.combine(checksum, inputStreams.get()[checkSumIdx].getChecksum().getValue(),
                partSize);
        }
        if (numberOfParts > 1) {
            checksum = ChecksumUtils.combine(checksum, inputStreams.get()[lastPartNumber].getChecksum().getValue(),
                lastPartSize);
        }

        if (expectedChecksum != checksum) {
            throw new CorruptedLocalFileException("Excepted checksum of local file did not match after upload");
        }
    }

    @Override
    public void close() throws IOException {
        assert inputStreams.get() != null : "Input streams are not yet set for multi stream upload";

        boolean closeStreamException = false;
        for (CheckedInputStream cis : inputStreams.get()) {
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
