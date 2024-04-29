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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.RateLimitingOffsetRangeInputStream;
import org.opensearch.common.blobstore.transfer.stream.ResettableCheckedInputStream;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.util.ByteUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.zip.CRC32;

import com.jcraft.jzlib.JZlib;

/**
 * RemoteTransferContainer is an encapsulation for managing file transfers.
 *
 * @opensearch.internal
 */
public class RemoteTransferContainer implements Closeable {

    private int numberOfParts;
    private long partSize;
    private long lastPartSize;

    private final long contentLength;
    private final SetOnce<Supplier<Long>[]> checksumSuppliers = new SetOnce<>();
    private final String fileName;
    private final String remoteFileName;
    private final boolean failTransferIfFileExists;
    private final WritePriority writePriority;
    private final Long expectedChecksum;
    private final OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier;
    private final boolean isRemoteDataIntegritySupported;
    private final AtomicBoolean readBlock = new AtomicBoolean();

    private static final Logger log = LogManager.getLogger(RemoteTransferContainer.class);

    /**
     * Construct a new RemoteTransferContainer object
     *
     * @param fileName                       Name of the local file
     * @param remoteFileName                 Name of the remote file
     * @param contentLength                  Total content length of the file to be uploaded
     * @param failTransferIfFileExists       A boolean to determine if upload has to be failed if file exists
     * @param writePriority                  The {@link WritePriority} of current upload
     * @param offsetRangeInputStreamSupplier A supplier to create OffsetRangeInputStreams
     * @param expectedChecksum               The expected checksum value for the file being uploaded. This checksum will be used for local or remote data integrity checks
     * @param isRemoteDataIntegritySupported A boolean to signify whether the remote repository supports server side data integrity verification
     */
    public RemoteTransferContainer(
        String fileName,
        String remoteFileName,
        long contentLength,
        boolean failTransferIfFileExists,
        WritePriority writePriority,
        OffsetRangeInputStreamSupplier offsetRangeInputStreamSupplier,
        Long expectedChecksum,
        boolean isRemoteDataIntegritySupported
    ) {
        this.fileName = fileName;
        this.remoteFileName = remoteFileName;
        this.contentLength = contentLength;
        this.failTransferIfFileExists = failTransferIfFileExists;
        this.writePriority = writePriority;
        this.offsetRangeInputStreamSupplier = offsetRangeInputStreamSupplier;
        this.expectedChecksum = expectedChecksum;
        this.isRemoteDataIntegritySupported = isRemoteDataIntegritySupported;
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
            this::finalizeUpload,
            isRemoteDataIntegrityCheckPossible(),
            isRemoteDataIntegrityCheckPossible() ? expectedChecksum : null
        );
    }

    // package-private for testing

    /**
     * This method is called to create the {@link StreamContext} object that will be used by the vendor plugin to
     * open streams during uploads. Calling this method won't actually create the streams, for that the consumer needs
     * to call {@link StreamContext#provideStream}
     *
     * @param partSize Part sizes of all parts apart from the last one, which is determined internally
     * @return The {@link StreamContext} object that will be used by the vendor plugin to retrieve streams during upload
     */
    StreamContext supplyStreamContext(long partSize) {
        try {
            return this.openMultipartStreams(partSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings({ "unchecked" })
    private StreamContext openMultipartStreams(long partSize) throws IOException {
        if (checksumSuppliers.get() != null) {
            throw new IOException("Multi-part streams are already created.");
        }

        this.partSize = partSize;
        this.lastPartSize = (contentLength % partSize) != 0 ? contentLength % partSize : partSize;
        this.numberOfParts = (int) ((contentLength % partSize) == 0 ? contentLength / partSize : (contentLength / partSize) + 1);
        Supplier<Long>[] suppliers = new Supplier[numberOfParts];
        checksumSuppliers.set(suppliers);

        return new StreamContext(getTransferPartStreamSupplier(), partSize, lastPartSize, numberOfParts);
    }

    private CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> getTransferPartStreamSupplier() {
        return ((partNo, size, position) -> {
            assert checksumSuppliers.get() != null : "expected container to be initialised";
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

    private LocalStreamSupplier<InputStreamContainer> getMultipartStreamSupplier(
        final int streamIdx,
        final long size,
        final long position
    ) {
        return () -> {
            try {
                OffsetRangeInputStream offsetRangeInputStream = offsetRangeInputStreamSupplier.get(size, position);
                if (offsetRangeInputStream instanceof RateLimitingOffsetRangeInputStream) {
                    RateLimitingOffsetRangeInputStream rangeIndexInputStream = (RateLimitingOffsetRangeInputStream) offsetRangeInputStream;
                    rangeIndexInputStream.setReadBlock(readBlock);
                }
                InputStream inputStream;
                if (isRemoteDataIntegrityCheckPossible() == false) {
                    ResettableCheckedInputStream resettableCheckedInputStream = new ResettableCheckedInputStream(
                        offsetRangeInputStream,
                        fileName
                    );
                    Objects.requireNonNull(checksumSuppliers.get())[streamIdx] = resettableCheckedInputStream::getChecksum;
                    inputStream = resettableCheckedInputStream;
                } else {
                    inputStream = offsetRangeInputStream;
                }

                return new InputStreamContainer(inputStream, size, position);
            } catch (IOException e) {
                log.error("Failed to create input stream", e);
                throw e;
            }
        };
    }

    private boolean isRemoteDataIntegrityCheckPossible() {
        return isRemoteDataIntegritySupported && Objects.nonNull(expectedChecksum);
    }

    private void finalizeUpload(boolean uploadSuccessful) throws IOException {
        if (isRemoteDataIntegrityCheckPossible()) {
            return;
        }

        if (uploadSuccessful && Objects.nonNull(expectedChecksum)) {
            long actualChecksum = getActualChecksum();
            if (actualChecksum != expectedChecksum) {
                throw new CorruptIndexException(
                    "Data integrity check done after upload for file "
                        + fileName
                        + " failed, actual checksum: "
                        + actualChecksum
                        + ", expected checksum: "
                        + expectedChecksum,
                    fileName
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
        Supplier<Long>[] ckSumSuppliers = Objects.requireNonNull(checksumSuppliers.get());
        long checksum = ckSumSuppliers[0].get();
        for (int checkSumIdx = 1; checkSumIdx < ckSumSuppliers.length - 1; checkSumIdx++) {
            checksum = JZlib.crc32_combine(checksum, ckSumSuppliers[checkSumIdx].get(), partSize);
        }
        if (numberOfParts > 1) {
            checksum = JZlib.crc32_combine(checksum, ckSumSuppliers[numberOfParts - 1].get(), lastPartSize);
        }

        return checksum;
    }

    @Override
    public void close() throws IOException {
        // Setting a read block on all streams ever created by the container.
        readBlock.set(true);
    }

    /**
     * Compute final checksum for IndexInput container checksum footer added by {@link CodecUtil}
     * @param indexInput IndexInput with checksum in footer
     * @param checksumBytesLength length of checksum bytes
     * @return final computed checksum of entire indexInput
     */
    public static long checksumOfChecksum(IndexInput indexInput, int checksumBytesLength) throws IOException {
        long storedChecksum = CodecUtil.retrieveChecksum(indexInput);
        CRC32 checksumOfChecksum = new CRC32();
        checksumOfChecksum.update(ByteUtils.toByteArrayBE(storedChecksum));
        return JZlib.crc32_combine(storedChecksum, checksumOfChecksum.getValue(), checksumBytesLength);
    }
}
