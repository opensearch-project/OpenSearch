/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.stream.StreamSupport;

/**
 * Generic FormatStoreDirectory implementation for non-Lucene formats.
 * Uses core Java APIs to provide directory functionality without Lucene dependencies.
 */
public class GenericStoreDirectory<T extends DataFormat> implements FormatStoreDirectory<T> {

    private final T dataFormat;
    private final Path directoryPath;
    private final Logger logger;

    /**
     * Creates a new GenericStoreDirectory
     * @param dataFormat the data format this directory handles
     * @param shardPath the shard path where directories should be created
     * @throws IOException if directory creation fails
     */
    public GenericStoreDirectory(
        T dataFormat,
        ShardPath shardPath
    ) throws IOException {
        this.dataFormat = dataFormat;
        this.directoryPath = shardPath.getDataPath().resolve(dataFormat.name());
        this.logger = LogManager.getLogger(dataFormat.name()+"."+shardPath.getShardId());

        Files.createDirectories(this.directoryPath);
    }


    @Override
    public T getDataFormat() {
        return dataFormat;
    }

    @Override
    public Path getDirectoryPath() {
        return directoryPath;
    }

    @Override
    public void initialize() throws IOException {
        logger.debug("Initialized GenericStoreDirectory for format: {}", dataFormat.name());
    }

    @Override
    public void cleanup() throws IOException {
        logger.debug("Cleaning up GenericStoreDirectory for format: {}", dataFormat.name());
    }

    @Override
    public FileMetadata[] listAll() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directoryPath)) {
            return StreamSupport.stream(stream.spliterator(), false)
                .map(Path::getFileName)
                .map(Path::toString)
                .filter(name -> !Files.isDirectory(directoryPath.resolve(name)))
                .map(fileName -> new FileMetadata(this.dataFormat.name(), fileName))  // Create FileMetadata with format + filename
                .toArray(FileMetadata[]::new);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to list files in directory",
                dataFormat,
                "listAll",
                directoryPath,
                e
            );
        }
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Path filePath = directoryPath.resolve(name);
        try {
            Files.deleteIfExists(filePath);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to delete file: " + name,
                dataFormat,
                "deleteFile",
                filePath,
                e
            );
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        Path filePath = directoryPath.resolve(name);
        try {
            return Files.size(filePath);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to get file length: " + name,
                dataFormat,
                "fileLength",
                filePath,
                e
            );
        }
    }

    @Override
    public OutputStream createOutput(String name) throws IOException {
        Path filePath = directoryPath.resolve(name);
        try {
            return Files.newOutputStream(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to create output stream for file: " + name,
                dataFormat,
                "createOutput",
                filePath,
                e
            );
        }
    }

    @Override
    public InputStream openInput(String name) throws IOException {
        Path filePath = directoryPath.resolve(name);
        try {
            return Files.newInputStream(filePath, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to open input stream for file: " + name,
                dataFormat,
                "openInput",
                filePath,
                e
            );
        }
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // For generic directories, sync each file by calling fsync
        for (String name : names) {
            Path filePath = directoryPath.resolve(name);
            if (Files.exists(filePath)) {
                try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.WRITE)) {
                    channel.force(true);
                } catch (IOException e) {
                    throw new MultiFormatStoreException(
                        "Failed to sync file: " + name,
                        dataFormat,
                        "sync",
                        filePath,
                        e
                    );
                }
            }
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        Path sourcePath = directoryPath.resolve(source);
        Path destPath = directoryPath.resolve(dest);
        try {
            Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to rename file from " + source + " to " + dest,
                dataFormat,
                "rename",
                sourcePath,
                e
            );
        }
    }

    @Override
    public long calculateChecksum(String fileName) throws IOException {
        Path filePath = directoryPath.resolve(fileName);
        try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
            return calculateGenericChecksum(inputStream);
        } catch (IOException e) {
            throw new MultiFormatStoreException(
                "Failed to calculate checksum for file: " + fileName,
                dataFormat,
                "calculateChecksum",
                filePath,
                e
            );
        }
    }

    /**
     * Calculates a generic CRC32 checksum for the given input stream
     * @param inputStream the input stream to calculate checksum for
     * @return the checksum as a string representation
     * @throws IOException if reading from the stream fails
     */
    private long calculateGenericChecksum(InputStream inputStream) throws IOException {
        java.util.zip.CRC32 crc32 = new java.util.zip.CRC32();
        byte[] buffer = new byte[8192];
        int bytesRead;

        while ((bytesRead = inputStream.read(buffer)) != -1) {
            crc32.update(buffer, 0, bytesRead);
        }

        return crc32.getValue();
    }

    @Override
    public void close() throws IOException {
        // No resources to close for generic directory
        logger.debug("Closed GenericStoreDirectory for format: {}", dataFormat.name());
    }

    @Override
    public String calculateUploadChecksum(String fileName) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        Path filePath = directoryPath.resolve(fileName);

        logger.debug("Calculating generic upload checksum: file={}, format={}, method=CRC32, filePath={}",
            fileName, dataFormat.name(), filePath);

        long startTime = System.nanoTime();

        try (InputStream inputStream = Files.newInputStream(filePath)) {
            long checksum = calculateGenericChecksum(inputStream);
            String checksumString = Long.toString(checksum);

            long calculationDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            logger.debug("Generic upload checksum calculated: file={}, format={}, checksum={}, durationMs={}",
                fileName, dataFormat.name(), checksumString, calculationDurationMs);

            return checksumString;

        } catch (IOException e) {
            long failureDurationMs = (System.nanoTime() - startTime) / 1_000_000;

            logger.error("Failed to calculate generic upload checksum: file={}, format={}, filePath={}, durationMs={}, error={}",
                fileName, dataFormat.name(), filePath, failureDurationMs, e.getMessage(), e);

            throw new MultiFormatStoreException(
                "Failed to calculate upload checksum for file: " + fileName,
                dataFormat,
                "calculateUploadChecksum",
                filePath,
                e
            );
        }
    }

    @Override
    public IndexInput openIndexInput(String name, IOContext context) throws IOException {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        Path filePath = directoryPath.resolve(name);

        logger.debug("Creating IndexInput for generic format: file={}, format={}, context={}, filePath={}",
            name, dataFormat.name(), context, filePath);

        try {
            // Validate file exists
            if (!Files.exists(filePath) || !Files.isRegularFile(filePath)) {
                throw new IOException("File does not exist or is not a regular file: " + filePath);
            }

            // Open FileChannel
            FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ);
            long fileSize = channel.size();

            // Create FileChannel-based IndexInput
            return new GenericFileChannelIndexInput(name, channel, fileSize, context);

        } catch (IOException e) {
            logger.error("Failed to create IndexInput for generic format: file={}, format={}, filePath={}, error={}",
                name, dataFormat.name(), filePath, e.getMessage(), e);

            throw new MultiFormatStoreException(
                "Failed to create IndexInput for file: " + name,
                dataFormat,
                "openIndexInput",
                filePath,
                e
            );
        }
    }

    /**
     * FileChannel-based IndexInput implementation that provides full Lucene compatibility.
     * This implementation mirrors NIOFSDirectory's internal IndexInput behavior.
     */
    private static class GenericFileChannelIndexInput extends IndexInput implements RandomAccessInput {
        private final FileChannel channel;
        private final long length;
        private final IOContext context;
        private final boolean isClone;
        private long position = 0;
        private volatile boolean closed = false;

        GenericFileChannelIndexInput(String name, FileChannel channel, long length, IOContext context) {
            this(name, channel, length, context, false);
        }

        private GenericFileChannelIndexInput(String name, FileChannel channel, long length, IOContext context, boolean isClone) {
            super("GenericFileChannelIndexInput(" + name + ")");
            this.channel = channel;
            this.length = length;
            this.context = context;
            this.isClone = isClone;
        }


        @Override
        public byte readByte() throws IOException {
            checkClosed();
            if (position >= length) {
                throw new EOFException("Read past EOF: position=" + position + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(1);
            int bytesRead = channel.read(buffer, position);
            if (bytesRead == -1) {
                throw new EOFException("Unexpected EOF at position: " + position);
            }

            position++;
            return buffer.get(0);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            checkClosed();
            if (position + len > length) {
                throw new EOFException("Read past EOF: position=" + position +
                                     ", requestedLen=" + len + ", fileLength=" + length);
            }

            ByteBuffer buffer = ByteBuffer.wrap(b, offset, len);
            int totalRead = 0;
            while (totalRead < len) {
                int bytesRead = channel.read(buffer, position + totalRead);
                if (bytesRead == -1) {
                    throw new EOFException("Unexpected EOF at position: " + (position + totalRead));
                }
                totalRead += bytesRead;
            }
            position += len;
        }

        @Override
        public long getFilePointer() {
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkClosed();
            if (pos < 0) {
                throw new IllegalArgumentException("Seek position cannot be negative: " + pos);
            }
            if (pos > length) {
                throw new EOFException("Seek past EOF: position=" + pos + ", length=" + length);
            }

            position = pos;
        }

        @Override
        public long length() {
            return length;
        }


        @Override
        public byte readByte(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos >= length) {
                throw new EOFException("Position out of bounds: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(1);
            int bytesRead = channel.read(buffer, pos);
            if (bytesRead == -1) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            return buffer.get(0);
        }

        @Override
        public short readShort(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Short.BYTES > length) {
                throw new EOFException("Position out of bounds: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, pos);
            if (bytesRead != Short.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getShort();  // ✅ Multi-byte random access
        }

        @Override
        public int readInt(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Integer.BYTES > length) {
                throw new EOFException("Position out of bounds: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, pos);
            if (bytesRead != Integer.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getInt();  // ✅ Multi-byte random access
        }

        @Override
        public long readLong(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Long.BYTES > length) {
                throw new EOFException("Position out of bounds: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, pos);
            if (bytesRead != Long.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getLong();
        }


        @Override
        public GenericFileChannelIndexInput clone() {
            checkClosed();
            try {
                // Create new FileChannel for the same file (like Lucene does)
                // Unfortunately, we can't get the original path from FileChannel easily,
                // so we'll share the same channel for now (requires careful resource management)
                GenericFileChannelIndexInput clone = new GenericFileChannelIndexInput(
                    toString(), channel, length, context, true
                );
                clone.position = this.position;

                return clone;

            } catch (Exception e) {
                throw new RuntimeException("Failed to clone IndexInput", e);
            }
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long sliceLength) throws IOException {
            checkClosed();
            if (offset < 0 || sliceLength < 0 || offset + sliceLength > length) {
                throw new IllegalArgumentException(
                    "Invalid slice: offset=" + offset + ", length=" + sliceLength +
                    ", fileLength=" + length
                );
            }

            // Create slice that shares the same FileChannel but with offset/length bounds
            return new SlicedGenericIndexInput(
                sliceDescription, channel, offset, sliceLength, context
            );
        }

        @Override
        public void close() throws IOException {
            if (!closed && !isClone) {  // Only close original, not clones
                channel.close();
                closed = true;
            }
        }

        private void checkClosed() {
            if (closed) {
                throw new RuntimeException("IndexInput is closed");
            }
        }
    }

    /**
     * Sliced version of GenericFileChannelIndexInput that operates within bounds
     */
    private static class SlicedGenericIndexInput extends IndexInput implements RandomAccessInput {
        private final FileChannel channel;
        private final long startOffset;
        private final long length;
        private final IOContext context;
        private long position = 0;
        private volatile boolean closed = false;

        SlicedGenericIndexInput(String name, FileChannel channel, long startOffset, long length, IOContext context) {
            super("SlicedGenericIndexInput(" + name + ")");
            this.channel = channel;
            this.startOffset = startOffset;
            this.length = length;
            this.context = context;
        }

        @Override
        public byte readByte() throws IOException {
            checkClosed();
            if (position >= length) {
                throw new EOFException("Read past EOF in slice: position=" + position + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(1);
            int bytesRead = channel.read(buffer, startOffset + position);
            if (bytesRead == -1) {
                throw new EOFException("Unexpected EOF at position: " + position);
            }

            position++;
            return buffer.get(0);
        }

        @Override
        public void readBytes(byte[] b, int offset, int len) throws IOException {
            checkClosed();
            if (position + len > length) {
                throw new EOFException("Read past EOF in slice: position=" + position +
                                     ", requestedLen=" + len + ", sliceLength=" + length);
            }

            ByteBuffer buffer = ByteBuffer.wrap(b, offset, len);
            int totalRead = 0;
            while (totalRead < len) {
                int bytesRead = channel.read(buffer, startOffset + position + totalRead);
                if (bytesRead == -1) {
                    throw new EOFException("Unexpected EOF at position: " + (position + totalRead));
                }
                totalRead += bytesRead;
            }
            position += len;
        }

        @Override
        public long getFilePointer() {
            return position;
        }

        @Override
        public void seek(long pos) throws IOException {
            checkClosed();
            if (pos < 0) {
                throw new IllegalArgumentException("Seek position cannot be negative: " + pos);
            }
            if (pos > length) {
                throw new EOFException("Seek past EOF in slice: position=" + pos + ", length=" + length);
            }

            position = pos;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public byte readByte(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos >= length) {
                throw new EOFException("Position out of bounds in slice: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(1);
            int bytesRead = channel.read(buffer, startOffset + pos);
            if (bytesRead == -1) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            return buffer.get(0);
        }

        @Override
        public short readShort(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Short.BYTES > length) {
                throw new EOFException("Position out of bounds in slice: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, startOffset + pos);
            if (bytesRead != Short.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getShort();
        }

        @Override
        public int readInt(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Integer.BYTES > length) {
                throw new EOFException("Position out of bounds in slice: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, startOffset + pos);
            if (bytesRead != Integer.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getInt();
        }

        @Override
        public long readLong(long pos) throws IOException {
            checkClosed();
            if (pos < 0 || pos + Long.BYTES > length) {
                throw new EOFException("Position out of bounds in slice: pos=" + pos + ", length=" + length);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            int bytesRead = channel.read(buffer, startOffset + pos);
            if (bytesRead != Long.BYTES) {
                throw new EOFException("Unexpected EOF at position: " + pos);
            }

            buffer.flip();
            return buffer.getLong();
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long sliceLength) throws IOException {
            checkClosed();
            if (offset < 0 || sliceLength < 0 || offset + sliceLength > length) {
                throw new IllegalArgumentException(
                    "Invalid slice: offset=" + offset + ", length=" + sliceLength +
                    ", parentLength=" + length
                );
            }

            return new SlicedGenericIndexInput(
                sliceDescription, channel, startOffset + offset, sliceLength, context
            );
        }

        @Override
        public SlicedGenericIndexInput clone() {
            checkClosed();
            SlicedGenericIndexInput clone = new SlicedGenericIndexInput(
                toString(), channel, startOffset, length, context
            );
            clone.position = this.position;
            return clone;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                // Slices don't own the channel, so don't close it
                closed = true;
            }
        }

        private void checkClosed() {
            if (closed) {
                throw new RuntimeException("IndexInput is closed");
            }
        }
    }
}
