/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Composite directory that coordinates multiple format-specific directories.
 * Routes file operations to appropriate format directories based on file type.
 * Implements both Directory and FormatStoreDirectory interfaces for compatibility.
 *
 * Follows the same plugin-based architecture pattern as CompositeIndexingExecutionEngine.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class CompositeStoreDirectory {

    private Any dataFormat;
    private final Path directoryPath;
    public final List<FormatStoreDirectory<?>> delegates = new ArrayList<>();
    public final HashMap<String, FormatStoreDirectory<?>> delegatesMap  = new HashMap<>();

    private final Logger logger;
    private final DirectoryFileTransferTracker directoryFileTransferTracker;
    private final ShardPath shardPath;

    /**
     * Constructor following CompositeIndexingExecutionEngine pattern exactly
     */
    public CompositeStoreDirectory(IndexSettings indexSettings, PluginsService pluginsService, Any dataFormats, ShardPath shardPath, Logger logger) {
        this.dataFormat = dataFormats;
        this.shardPath = shardPath;
        this.logger = logger;
        this.directoryFileTransferTracker = new DirectoryFileTransferTracker();
        this.directoryPath = shardPath.getDataPath();
        try {
            DataSourcePlugin  plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream().findAny().orElseThrow(() -> new IllegalArgumentException("dataformat [" + DataFormat.TEXT + "] is not registered."));
            delegates.add(plugin.createFormatStoreDirectory(indexSettings, shardPath));
            delegatesMap.put(plugin.getDataFormat().name(), delegates.getLast());
        } catch (NullPointerException | IOException e) {
            delegatesMap.put("error", null);
        }

        logger.debug("Created CompositeStoreDirectory with {} format directories",
            delegates.size());
    }

    /**
     * Simplified constructor for auto-discovery (like CompositeIndexingExecutionEngine)
     */
    public CompositeStoreDirectory(IndexSettings indexSettings, PluginsService pluginsService, ShardPath shardPath, Logger logger) {
        this.shardPath = shardPath;
        this.logger = logger;
        this.directoryFileTransferTracker = new DirectoryFileTransferTracker();
        this.directoryPath = shardPath.getDataPath();

        try {
            DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream()
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("dataformat is not registered."));
            delegates.add(plugin.createFormatStoreDirectory(indexSettings, shardPath));
            delegatesMap.put(plugin.getDataFormat().name(), delegates.get(delegates.size() - 1));
        } catch (NullPointerException | IOException e) {
                throw new RuntimeException("Failed to create fallback directory", e);
        }
    }

    // ===== FormatStoreDirectory<Any> Implementation =====

    public Any getDataFormat() {
        return dataFormat;
    }

    public Path getDirectoryPath() {
        // Return the shard path as this is the composite root
        return shardPath.getDataPath();
    }

    public void initialize() throws IOException {
        // Initialize all delegates
        for (FormatStoreDirectory<?> delegate : delegates) {
            delegate.initialize();
        }
    }
    public void cleanup() throws IOException {
        // Cleanup all delegates
        for (FormatStoreDirectory<?> delegate : delegates) {
            delegate.cleanup();
        }
    }

    /**
     * Gets the FormatStoreDirectory for a specific data format by name.
     * Uses the delegatesMap to find the appropriate directory delegate.
     *
     * @param dataFormatName the name of the data format (e.g., "lucene", "parquet", "text")
     * @return the FormatStoreDirectory that handles the specified format
     * @throws IllegalArgumentException if no directory is found for the format
     */
    public FormatStoreDirectory<?> getDirectoryForFormat(String dataFormatName) {
        logger.trace("Format routing request: searching for directory for format '{}'", dataFormatName);

        FormatStoreDirectory<?> directory = delegatesMap.get(dataFormatName);

        if (directory == null) {

            if(dataFormatName.equalsIgnoreCase("TempMetadata") && !delegates.isEmpty())
            {
                return delegates.getFirst();
            }
            List<String> availableFormats = new ArrayList<>(delegatesMap.keySet());

            logger.error("Format routing failed: requested format '{}' not found. Available formats: {}. " +
                    "This indicates a configuration issue or missing format plugin.",
                dataFormatName, availableFormats);

            throw new IllegalArgumentException(
                String.format("No directory found for format '%s'. Available formats: %s",
                    dataFormatName, availableFormats)
            );
        }

        logger.debug("Format routing successful: format '{}' routed to directory type '{}'",
            dataFormatName, directory.getClass().getSimpleName());

        return directory;
    }

    // Directory interface implementation with routing
    public FileMetadata[] listAll() throws IOException {
        Set<FileMetadata> allFiles = new HashSet<>();
        for (FormatStoreDirectory directory : delegates) {
            allFiles.addAll(Arrays.asList(directory.listAll()));
        }
        return allFiles.toArray(new FileMetadata[0]);
    }

    public void sync(Collection<FileMetadata> fileMetadataList) throws IOException {
        // Group files by directory and sync each directory
        Map<FormatStoreDirectory, List<String>> filesByDirectory = new HashMap<>();

        for (var fileMetadata : fileMetadataList) {
            FormatStoreDirectory directory = getDirectoryForFormat(fileMetadata.dataFormat());
            filesByDirectory.computeIfAbsent(directory, k -> new ArrayList<>()).add(fileMetadata.file());
        }

        for (Map.Entry<FormatStoreDirectory, List<String>> entry : filesByDirectory.entrySet()) {
            entry.getKey().sync(entry.getValue());
        }
    }

    public void syncMetaData() throws IOException {
        // Sync metadata for all directories
        for (FormatStoreDirectory directory : delegates) {
            directory.syncMetaData();
        }
    }

    public void rename(FileMetadata sourceFile, FileMetadata destinationFile) throws IOException {
        FormatStoreDirectory sourceDir = getDirectoryForFormat(sourceFile.dataFormat());
        FormatStoreDirectory destDir = getDirectoryForFormat(destinationFile.dataFormat());

        if (sourceDir != destDir) {
            throw new IOException("Cannot rename file across different format directories: " +
                                sourceFile.file() + " (" + sourceDir.getDataFormat().name() + ") -> " +
                                destinationFile.file() + " (" + destDir.getDataFormat().name() + ")");
        }

        sourceDir.rename(sourceFile.file(), destinationFile.file());
    }

    public Lock obtainLock(FileMetadata fileMetadata) throws IOException {
        // For lock files, try to route to appropriate directory or use first available
        FormatStoreDirectory directory;
        try {
            directory = getDirectoryForFormat(fileMetadata.dataFormat());
        } catch (IllegalArgumentException e) {
            // If no format accepts the lock file, use the first available directory
            directory = delegates.get(0);
            logger.debug("No format accepts lock file {}, using first available directory: {}",
                fileMetadata.file(), directory.getDataFormat().name());
        }

        // For Lucene directory, delegate to the wrapped Directory for proper lock handling
        if (directory instanceof LuceneStoreDirectory) {
            LuceneStoreDirectory luceneDir = (LuceneStoreDirectory) directory;
            return luceneDir.getWrappedDirectory().obtainLock(fileMetadata.file());
        }

        // For generic directories, create a file-based lock
        GenericFileLock lock = new GenericFileLock(directory.getDirectoryPath().resolve(fileMetadata.file() + ".lock"));
        lock.acquire(); // Actually acquire the lock
        return lock;
    }

    public void close() throws IOException {
        IOException firstException = null;

        for (FormatStoreDirectory directory : delegates) {
            try {
                directory.close();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }

        if (firstException != null) {
            throw firstException;
        }
    }

    public long getChecksumOfLocalFile(FileMetadata fileMetadata) throws IOException {
        logger.debug("Getting checksum of local file: {}", fileMetadata.file());
        return calculateChecksum(fileMetadata);
    }

    public Set<String> getPendingDeletions() throws IOException {
        // Aggregate pending deletions from all format directories
        // For now, return empty set as FormatStoreDirectory doesn't track pending deletions
        return Set.of();
    }

    // ===== FileMetadata-based Directory API Methods =====

    /**
     * Returns the byte length of a file using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @return the length of the file in bytes
     * @throws IOException if the file cannot be accessed
     */
    public long fileLength(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.fileLength(fileMetadata.file());
    }

    /**
     * Deletes a file using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @throws IOException if the file exists but could not be deleted
     */
    public void deleteFile(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        formatDirectory.deleteFile(fileMetadata.file());
    }

    /**
     * Opens an IndexInput for reading using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @param context IOContext providing performance hints for the operation
     * @return IndexInput for reading from the file
     * @throws IOException if the IndexInput cannot be created or file does not exist
     */
    public IndexInput openInput(FileMetadata fileMetadata, IOContext context) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.openIndexInput(fileMetadata.file(), context);
    }

    /**
     * Creates an IndexOutput for writing using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @param context IOContext providing performance hints for the operation
     * @return IndexOutput for writing to the file
     * @throws IOException if the IndexOutput cannot be created
     */
    public IndexOutput createOutput(FileMetadata fileMetadata, IOContext context) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        OutputStream outputStream = formatDirectory.createOutput(fileMetadata.file());
        return new OutputStreamIndexOutput(outputStream, fileMetadata.file());
    }

    /**
     * Copies a file from another directory using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @param source the source Directory to copy from
     * @param context IOContext providing performance hints for the operation
     * @throws IOException if the copy operation fails
     */
    public void copyFrom(FileMetadata fileMetadata, RemoteSegmentStoreDirectory source, IOContext context) throws IOException {
        FormatStoreDirectory targetDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        String fileName = fileMetadata.file();

        logger.debug("Copying file {} to format directory: {}", fileName, targetDirectory.getDataFormat().name());

        try (IndexInput input = source.openInput(fileMetadata, context);
             IndexOutput output = createOutput(fileMetadata, context)) {

            output.copyBytes(input, input.length());
        }
    }

    /**
     * Calculates checksum using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @return the checksum as a long value
     * @throws IOException if checksum calculation fails
     */
    public long calculateChecksum(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.calculateChecksum(fileMetadata.file());
    }

    /**
     * Calculates upload checksum using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @return checksum string in format-specific representation
     * @throws IOException if checksum calculation fails
     */
    public String calculateUploadChecksum(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.calculateUploadChecksum(fileMetadata.file());
    }
}

/**
 * Adapter class that converts OutputStream to Lucene IndexOutput
 */
class OutputStreamIndexOutput extends IndexOutput {
    private final OutputStream outputStream;
    private final String name;
    private long bytesWritten = 0;

    OutputStreamIndexOutput(OutputStream outputStream, String name) {
        super("OutputStreamIndexOutput(" + name + ")", name);
        this.outputStream = outputStream;
        this.name = name;
    }

    @Override
    public void writeByte(byte b) throws IOException {
        outputStream.write(b & 0xFF);
        bytesWritten++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        outputStream.write(b, offset, length);
        bytesWritten += length;
    }

    @Override
    public long getFilePointer() {
        return bytesWritten;
    }

    @Override
    public long getChecksum() throws IOException {
        // Generic implementation - checksum not supported
        throw new UnsupportedOperationException("Checksum not supported for generic output streams");
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}

/**
 * Adapter class that converts InputStream to Lucene IndexInput
 * Note: This implementation requires the InputStream to be created from a file path
 * so that we can determine the file length.
 */
class InputStreamIndexInput extends IndexInput {
    private final InputStream inputStream;
    private final String name;
    private final long length;
    private long position = 0;

    /**
     * Creates an IndexInput adapter for the given InputStream and file path.
     * The filePath is used to determine the file length.
     */
    static InputStreamIndexInput create(InputStream inputStream, String name, Path filePath) throws IOException {
        long fileLength = Files.size(filePath);
        return new InputStreamIndexInput(inputStream, name, fileLength);
    }

    private InputStreamIndexInput(InputStream inputStream, String name, long length) throws IOException {
        super("InputStreamIndexInput(" + name + ")");
        this.inputStream = inputStream;
        this.name = name;
        this.length = length;
    }

    @Override
    public byte readByte() throws IOException {
        if (position >= length) {
            throw new IOException("Read past EOF: " + name);
        }
        int b = inputStream.read();
        if (b == -1) {
            throw new IOException("Unexpected EOF: " + name);
        }
        position++;
        return (byte) b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        if (position + len > length) {
            throw new IOException("Read past EOF: " + name);
        }

        int totalRead = 0;
        while (totalRead < len) {
            int read = inputStream.read(b, offset + totalRead, len - totalRead);
            if (read == -1) {
                throw new IOException("Unexpected EOF: " + name);
            }
            totalRead += read;
        }
        position += len;
    }

    @Override
    public long getFilePointer() {
        return position;
    }

    @Override
    public void seek(long pos) throws IOException {
        throw new UnsupportedOperationException("Seek not supported for generic input streams");
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        throw new UnsupportedOperationException("Slice not supported for generic input streams");
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }
}

/**
 * Generic file lock implementation using Java NIO file locking
 */
class GenericFileLock extends Lock {
    private final Path lockFile;
    private FileChannel channel;
    private FileLock fileLock;

    GenericFileLock(Path lockFile) {
        this.lockFile = lockFile;
    }

    @Override
    public void ensureValid() throws IOException {
        if (fileLock == null || !fileLock.isValid()) {
            throw new IOException("Lock is not valid: " + lockFile);
        }
    }

    @Override
    public void close() throws IOException {
        if (fileLock != null) {
            try {
                fileLock.release();
            } finally {
                fileLock = null;
            }
        }

        if (channel != null) {
            try {
                channel.close();
            } finally {
                channel = null;
            }
        }

        // Clean up lock file
        Files.deleteIfExists(lockFile);
    }

    /**
     * Acquire the lock
     */
    void acquire() throws IOException {
        // Create parent directories if needed
        Files.createDirectories(lockFile.getParent());

        // Open channel and acquire lock
        channel = FileChannel.open(lockFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING);

        fileLock = channel.tryLock();
        if (fileLock == null) {
            channel.close();
            throw new IOException("Could not acquire lock: " + lockFile);
        }
    }
}
