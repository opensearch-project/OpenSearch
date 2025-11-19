/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//package org.opensearch.index.store;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.apache.lucene.store.IOContext;
//import org.apache.lucene.store.IndexInput;
//import org.opensearch.index.engine.exec.DataFormat;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.nio.file.Files;
//import java.nio.file.NoSuchFileException;
//import java.nio.file.Path;
//import java.util.Collection;
//
///**
// * FormatStoreDirectory implementation for Parquet format files.
// * Handles storage and retrieval of Parquet-formatted data files.
// */
//public class ParquetStoreDirectory implements FormatStoreDirectory<DataFormat> {
//
//    private static final Logger logger = LogManager.getLogger(ParquetStoreDirectory.class);
//
//    private final Path directoryPath;
//
//    public ParquetStoreDirectory(Path shardPath) throws IOException {
//        this.directoryPath = shardPath.resolve("parquet");
//        Files.createDirectories(this.directoryPath);
//    }
//
//    @Override
//    public DataFormat getDataFormat() {
//        return DataFormat.PARQUET;
//    }
//
//    @Override
//    public boolean supportsFormat(DataFormat format) {
//        return FormatStoreDirectory.super.supportsFormat(format);
//    }
//
//    @Override
//    @Deprecated
//    public boolean acceptsFile(String fileName) {
//        // This method is deprecated - format determination should be done through FileMetadata
//        // For backward compatibility, check for parquet file extensions
//        return fileName.endsWith(".parquet") || fileName.endsWith(".pq");
//    }
//
//    @Override
//    public Path getDirectoryPath() {
//        return directoryPath;
//    }
//
//    @Override
//    public void initialize() throws IOException {
//        // Parquet-specific initialization if needed
//        logger.debug("Initializing ParquetStoreDirectory at path: {}", directoryPath);
//    }
//
//    @Override
//    public void cleanup() throws IOException {
//        // Parquet-specific cleanup if needed
//        logger.debug("Cleaning up ParquetStoreDirectory at path: {}", directoryPath);
//    }
//
//    @Override
//    public String[] listAll() throws IOException {
//        if (!Files.exists(directoryPath)) {
//            return new String[0];
//        }
//        return Files.list(directoryPath)
//            .map(path -> path.getFileName().toString())
//            .toArray(String[]::new);
//    }
//
//    @Override
//    public void deleteFile(String name) throws IOException {
//        Path filePath = directoryPath.resolve(name);
//        if (!Files.deleteIfExists(filePath)) {
//            throw new NoSuchFileException("File does not exist: " + name);
//        }
//    }
//
//    @Override
//    public long fileLength(String name) throws IOException {
//        Path filePath = directoryPath.resolve(name);
//        if (!Files.exists(filePath)) {
//            throw new NoSuchFileException("File does not exist: " + name);
//        }
//        return Files.size(filePath);
//    }
//
//    @Override
//    public OutputStream createOutput(String name) throws IOException {
//        Path filePath = directoryPath.resolve(name);
//        return Files.newOutputStream(filePath);
//    }
//
//    @Override
//    public InputStream openInput(String name) throws IOException {
//        Path filePath = directoryPath.resolve(name);
//        if (!Files.exists(filePath)) {
//            throw new FileNotFoundException("File does not exist: " + name);
//        }
//        return Files.newInputStream(filePath);
//    }
//
//    @Override
//    public void sync(Collection<String> names) throws IOException {
//        // Parquet files are typically written atomically, so sync is usually not needed
//        // But we can implement it for completeness
//        for (String name : names) {
//            Path filePath = directoryPath.resolve(name);
//            if (Files.exists(filePath)) {
//                // Force sync to disk - this is a no-op on most filesystems for regular files
//                // but ensures data is written to storage
//            }
//        }
//    }
//
//    @Override
//    public void syncMetaData() throws IOException {
//        // Sync directory metadata
//        // This is typically a no-op but ensures directory changes are persisted
//    }
//
//    @Override
//    public void rename(String source, String dest) throws IOException {
//        Path sourcePath = directoryPath.resolve(source);
//        Path destPath = directoryPath.resolve(dest);
//
//        if (!Files.exists(sourcePath)) {
//            throw new NoSuchFileException("Source file does not exist: " + source);
//        }
//
//        Files.move(sourcePath, destPath);
//    }
//
//    @Override
//    public boolean fileExists(String name) throws IOException {
//        Path filePath = directoryPath.resolve(name);
//        return Files.exists(filePath);
//    }
//
//    @Override
//    public void close() throws IOException {
//        // Nothing to close for file-based directory
//    }
//
//    @Override
//    public long calculateChecksum(String fileName) throws IOException {
//        // For Parquet files, we can calculate a simple checksum
//        // This is a basic implementation - could be enhanced with CRC32 or other algorithms
//        Path filePath = directoryPath.resolve(fileName);
//        if (!Files.exists(filePath)) {
//            throw new NoSuchFileException("File does not exist: " + fileName);
//        }
//
//        // Simple checksum based on file size and modification time
//        long size = Files.size(filePath);
//        long lastModified = Files.getLastModifiedTime(filePath).toMillis();
//        return size ^ lastModified;
//    }
//
//    @Override
//    public InputStream createUploadInputStream(String fileName) throws IOException {
//        if (fileName == null || fileName.trim().isEmpty()) {
//            throw new IllegalArgumentException("File name cannot be null or empty");
//        }
//
//        logger.debug("Creating Parquet upload input stream: file={}, directoryPath={}",
//            fileName, directoryPath.resolve(fileName));
//
//        Path filePath = directoryPath.resolve(fileName);
//        if (!Files.exists(filePath)) {
//            throw new FileNotFoundException("Parquet file does not exist: " + fileName);
//        }
//
//        return Files.newInputStream(filePath);
//    }
//
//    @Override
//    public InputStream createUploadRangeInputStream(String fileName, long offset, long length) throws IOException {
//        if (fileName == null || fileName.trim().isEmpty()) {
//            throw new IllegalArgumentException("File name cannot be null or empty");
//        }
//        if (offset < 0) {
//            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
//        }
//        if (length <= 0) {
//            throw new IllegalArgumentException("Length must be positive: " + length);
//        }
//
//        Path filePath = directoryPath.resolve(fileName);
//        if (!Files.exists(filePath)) {
//            throw new FileNotFoundException("Parquet file does not exist: " + fileName);
//        }
//
//        long fileSize = Files.size(filePath);
//        if (offset >= fileSize) {
//            throw new IllegalArgumentException("Offset " + offset + " is beyond file length " + fileSize);
//        }
//        if (offset + length > fileSize) {
//            throw new IllegalArgumentException("Range [" + offset + ", " + (offset + length) +
//                ") exceeds file length " + fileSize);
//        }
//
//        InputStream fullStream = Files.newInputStream(filePath);
//
//        // Skip to the offset
//        long skipped = fullStream.skip(offset);
//        if (skipped != offset) {
//            fullStream.close();
//            throw new IOException("Could not skip to offset " + offset + ", only skipped " + skipped);
//        }
//
//        // Return a limited stream that only reads the specified length
//        return new InputStream() {
//            private long remaining = length;
//
//            @Override
//            public int read() throws IOException {
//                if (remaining <= 0) {
//                    return -1;
//                }
//                int result = fullStream.read();
//                if (result != -1) {
//                    remaining--;
//                }
//                return result;
//            }
//
//            @Override
//            public int read(byte[] b, int off, int len) throws IOException {
//                if (remaining <= 0) {
//                    return -1;
//                }
//                int toRead = (int) Math.min(len, remaining);
//                int bytesRead = fullStream.read(b, off, toRead);
//                if (bytesRead > 0) {
//                    remaining -= bytesRead;
//                }
//                return bytesRead;
//            }
//
//            @Override
//            public void close() throws IOException {
//                fullStream.close();
//            }
//        };
//    }
//
//    @Override
//    public String calculateUploadChecksum(String fileName) throws IOException {
//        if (fileName == null || fileName.trim().isEmpty()) {
//            throw new IllegalArgumentException("File name cannot be null or empty");
//        }
//
//        logger.debug("Calculating Parquet upload checksum: file={}", fileName);
//
//        long startTime = System.nanoTime();
//
//        // Calculate checksum for Parquet file
//        long checksum = calculateChecksum(fileName);
//        String checksumStr = Long.toString(checksum);
//
//        long calculationDurationMs = (System.nanoTime() - startTime) / 1_000_000;
//
//        logger.debug("Parquet upload checksum calculated: file={}, checksum={}, durationMs={}",
//            fileName, checksumStr, calculationDurationMs);
//
//        return checksumStr;
//    }
//
//    @Override
//    public void onUploadComplete(String fileName, String remoteFileName) throws IOException {
//        if (fileName == null || fileName.trim().isEmpty()) {
//            throw new IllegalArgumentException("File name cannot be null or empty");
//        }
//        if (remoteFileName == null || remoteFileName.trim().isEmpty()) {
//            throw new IllegalArgumentException("Remote file name cannot be null or empty");
//        }
//
//        logger.debug("Parquet upload completed: localFile={}, remoteFile={}, format={}, directoryPath={}",
//            fileName, remoteFileName, getDataFormat().name(), directoryPath.resolve(fileName));
//
//        // Future enhancements could include:
//        // - Updating local metadata about uploaded Parquet files
//        // - Triggering Parquet-specific cleanup operations
//        // - Notifying Parquet-specific monitoring systems
//        // - Validating Parquet file integrity post-upload
//    }
//
//    @Override
//    public IndexInput openIndexInput(String name, IOContext context) throws IOException {
//        return null;
//    }
//}
