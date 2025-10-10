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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

/**
 * FormatStoreDirectory implementation for Lucene format files.
 * Wraps existing Lucene Directory to maintain full Lucene compatibility.
 */
public class LuceneStoreDirectory implements FormatStoreDirectory<DataFormat> {

    private static final Logger logger = LogManager.getLogger(LuceneStoreDirectory.class);

    private final Directory wrappedDirectory;
    private final Path directoryPath;

    public LuceneStoreDirectory(
        Path shardPath,
        Directory directory
    ) throws IOException {
        this.directoryPath = shardPath.resolve("lucene");
        Files.createDirectories(this.directoryPath);

        // Use the provided Directory directly
        this.wrappedDirectory = directory;
    }

    @Override
    public DataFormat getDataFormat() {
        return DataFormat.LUCENE;
    }

    @Override
    @Deprecated
    public boolean acceptsFile(String fileName) {
        // This method is deprecated - format determination should be done through FileMetadata
        // For backward compatibility, return true for all files since Lucene is the default format
        return true;
    }

    @Override
    public Path getDirectoryPath() {
        return directoryPath;
    }

    @Override
    public void initialize() throws IOException {
        // Lucene-specific initialization if needed
    }

    @Override
    public void cleanup() throws IOException {
        // Lucene-specific cleanup if needed
    }

    // Implement FormatStoreDirectory methods by delegating to wrappedDirectory
    @Override
    public FileMetadata[] listAll() throws IOException {
        String[] fileNames = wrappedDirectory.listAll();
        FileMetadata[] fileMetadataArray = new FileMetadata[fileNames.length];

        String dataFormat = getDataFormat().toString(); // "LUCENE"

        for (int i = 0; i < fileNames.length; i++) {
            fileMetadataArray[i] = new FileMetadata(dataFormat, "", fileNames[i]);
        }

        return fileMetadataArray;
    }

    @Override
    public void deleteFile(String name) throws IOException {
        wrappedDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return wrappedDirectory.fileLength(name);
    }

    @Override
    public OutputStream createOutput(String name) throws IOException {
        // Convert Lucene IndexOutput to OutputStream
        IndexOutput indexOutput = wrappedDirectory.createOutput(name, IOContext.DEFAULT);
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                indexOutput.writeByte((byte) b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                indexOutput.writeBytes(b, off, len);
            }

            @Override
            public void close() throws IOException {
                indexOutput.close();
            }
        };
    }

    @Override
    public InputStream openInput(String name) throws IOException {
        // Convert Lucene IndexInput to InputStream
        IndexInput indexInput = wrappedDirectory.openInput(name, IOContext.DEFAULT);
        return new InputStream() {
            @Override
            public int read() throws IOException {
                if (indexInput.getFilePointer() >= indexInput.length()) {
                    return -1;
                }
                return indexInput.readByte() & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                long remaining = indexInput.length() - indexInput.getFilePointer();
                if (remaining <= 0) {
                    return -1;
                }

                int toRead = (int) Math.min(len, remaining);
                indexInput.readBytes(b, off, toRead);
                return toRead;
            }

            @Override
            public void close() throws IOException {
                indexInput.close();
            }
        };
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        wrappedDirectory.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        wrappedDirectory.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        wrappedDirectory.rename(source, dest);
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        try {
            wrappedDirectory.fileLength(name);
            return true;
        } catch (FileNotFoundException | NoSuchFileException e) {
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        wrappedDirectory.close();
    }

    /**
     * Returns the wrapped Lucene Directory for direct access
     * @return the wrapped Directory instance
     */
    public Directory getWrappedDirectory() {
        return wrappedDirectory;
    }

    @Override
    public long calculateChecksum(String fileName) throws IOException {
        try (IndexInput indexInput = wrappedDirectory.openInput(fileName, IOContext.READONCE)) {
            return CodecUtil.retrieveChecksum(indexInput);
        }
    }

    // Upload-specific methods for remote segment upload

    @Override
    public InputStream createUploadInputStream(String fileName) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        logger.debug("Creating Lucene upload input stream: file={}, directoryPath={}",
            fileName, directoryPath.resolve(fileName));

        // Convert Lucene IndexInput to standard InputStream using inline adapter
        IndexInput indexInput = wrappedDirectory.openInput(fileName, IOContext.DEFAULT);

        logger.trace("Lucene upload input stream created successfully: file={}, indexInputType={}",
            fileName, indexInput.getClass().getSimpleName());

        return new InputStream() {
            @Override
            public int read() throws IOException {
                if (indexInput.getFilePointer() >= indexInput.length()) {
                    return -1; // EOF
                }
                return indexInput.readByte() & 0xFF;
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (indexInput.getFilePointer() >= indexInput.length()) {
                    return -1; // EOF
                }

                long remaining = indexInput.length() - indexInput.getFilePointer();
                int toRead = (int) Math.min(len, remaining);

                indexInput.readBytes(b, off, toRead);
                return toRead;
            }

            @Override
            public long skip(long n) throws IOException {
                long remaining = indexInput.length() - indexInput.getFilePointer();
                long toSkip = Math.min(n, remaining);

                if (toSkip > 0) {
                    indexInput.seek(indexInput.getFilePointer() + toSkip);
                }

                return toSkip;
            }

            @Override
            public void close() throws IOException {
                indexInput.close();
            }
        };
    }

    @Override
    public InputStream createUploadRangeInputStream(String fileName, long offset, long length) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative: " + offset);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("Length must be positive: " + length);
        }

        // Create range-based stream from Lucene IndexInput using existing OffsetRangeIndexInputStream
        IndexInput indexInput = wrappedDirectory.openInput(fileName, IOContext.DEFAULT);

        // Validate that the range is within file bounds
        long fileLength = indexInput.length();
        if (offset >= fileLength) {
            indexInput.close();
            throw new IllegalArgumentException("Offset " + offset + " is beyond file length " + fileLength);
        }
        if (offset + length > fileLength) {
            indexInput.close();
            throw new IllegalArgumentException("Range [" + offset + ", " + (offset + length) +
                ") exceeds file length " + fileLength);
        }

        try {
            return new org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream(
                indexInput, length, offset);
        } catch (IOException e) {
            // Clean up IndexInput if OffsetRangeIndexInputStream creation fails
            try {
                indexInput.close();
            } catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
    }

    @Override
    public String calculateUploadChecksum(String fileName) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }

        logger.debug("Calculating Lucene upload checksum: file={}, method=checksum-of-checksum", fileName);

        long startTime = System.nanoTime();

        // Use existing Lucene checksum calculation logic
        String checksum;
        try (IndexInput indexInput = wrappedDirectory.openInput(fileName, IOContext.READONCE)) {
            checksum = Long.toString(org.apache.lucene.codecs.CodecUtil.retrieveChecksum(indexInput));
        } catch (Exception e) {
            throw new IOException("Failed to calculate Lucene checksum for " + fileName, e);
        }

        long calculationDurationMs = (System.nanoTime() - startTime) / 1_000_000;

        logger.debug("Lucene upload checksum calculated: file={}, checksum={}, durationMs={}",
            fileName, checksum, calculationDurationMs);

        return checksum;
    }

    @Override
    public void onUploadComplete(String fileName, String remoteFileName) throws IOException {
        if (fileName == null || fileName.trim().isEmpty()) {
            throw new IllegalArgumentException("File name cannot be null or empty");
        }
        if (remoteFileName == null || remoteFileName.trim().isEmpty()) {
            throw new IllegalArgumentException("Remote file name cannot be null or empty");
        }

        logger.debug("Lucene upload completed: localFile={}, remoteFile={}, format={}, directoryPath={}",
            fileName, remoteFileName, fileName, directoryPath.resolve(fileName));

        // Future enhancements could include:
        // - Updating local metadata about uploaded files
        // - Triggering Lucene-specific cleanup operations
        // - Notifying Lucene-specific monitoring systems
        // - Validating Lucene file integrity post-upload
    }

    @Override
    public IndexInput openIndexInput(String name, IOContext context) throws IOException {
        return wrappedDirectory.openInput(name, context);
    }

}
