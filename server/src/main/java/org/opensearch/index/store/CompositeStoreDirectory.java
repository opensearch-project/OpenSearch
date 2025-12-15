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
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.index.shard.ShardPath.INDEX_FOLDER_NAME;
import static org.opensearch.index.shard.ShardPath.METADATA_FOLDER_NAME;

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
public class CompositeStoreDirectory implements Closeable {

    public final List<FormatStoreDirectory<?>> delegates = new ArrayList<>();
    public final HashMap<String, FormatStoreDirectory<?>> delegatesMap  = new HashMap<>();

    private final Logger logger;
    private final DirectoryFileTransferTracker directoryFileTransferTracker;

    /**
     * Simplified constructor for auto-discovery (like CompositeIndexingExecutionEngine)
     */
    public CompositeStoreDirectory(IndexSettings indexSettings, PluginsService pluginsService, ShardPath shardPath, Logger logger) {
        this.logger = logger;
        this.directoryFileTransferTracker = new DirectoryFileTransferTracker();

        try {
            FormatStoreDirectory<?> metadataDirectory = createMetadataDirectory(shardPath);
            delegatesMap.put("metadata", metadataDirectory);
            logger.debug("Created metadata directory pointing to: {}", shardPath.resolveIndex());

            pluginsService.filterPlugins(DataSourcePlugin.class).forEach(plugin -> {
                try {
                    FormatStoreDirectory<?> formatDir = plugin.createFormatStoreDirectory(indexSettings, shardPath);
                    delegates.add(formatDir);
                    delegatesMap.put(plugin.getDataFormat().name(), formatDir);
                } catch (IOException e) {
                    logger.error("Failed to create FormatStoreDirectory for format: {}", plugin.getDataFormat().name(), e);
                    throw new RuntimeException("Failed to create format directory for " + plugin.getDataFormat().name(), e);
                }
            });

            if (delegates.isEmpty()) {
                throw new IllegalArgumentException("No dataformat plugins registered.");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create CompositeStoreDirectory", e);
        }
    }

    /**
     * Creates a metadata directory that points to the base Lucene directory where segments_N files are stored.
     * This directory is at {@code <dataPath>/lucene/} and always exists regardless of active data formats.
     */
    private FormatStoreDirectory<?> createMetadataDirectory(ShardPath shardPath) throws IOException {
        // Create FSDirectory pointing to <dataPath>/lucene/ where segments_N files live
        Path luceneIndexPath = shardPath.resolveIndex(); // Returns <dataPath>/lucene/
        Directory luceneDirectory = FSDirectory.open(luceneIndexPath);
        return new LuceneStoreDirectory(luceneIndexPath, luceneDirectory);
    }

    public void initialize() throws IOException {
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

            if(dataFormatName.equalsIgnoreCase(METADATA_FOLDER_NAME) && !delegates.isEmpty())
            {
                return delegatesMap.get(INDEX_FOLDER_NAME);
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
        for (FormatStoreDirectory<?> directory : delegates) {
            allFiles.addAll(Arrays.asList(directory.listAll()));
        }
        return allFiles.toArray(new FileMetadata[0]);
    }

    public void sync(Collection<FileMetadata> fileMetadataList) throws IOException {
        // Group files by directory and sync each directory
        Map<FormatStoreDirectory<?>, List<String>> filesByDirectory = new HashMap<>();

        for (var fileMetadata : fileMetadataList) {
            FormatStoreDirectory<?> directory = getDirectoryForFormat(fileMetadata.dataFormat());
            filesByDirectory.computeIfAbsent(directory, k -> new ArrayList<>()).add(fileMetadata.file());
        }

        for (Map.Entry<FormatStoreDirectory<?>, List<String>> entry : filesByDirectory.entrySet()) {
            entry.getKey().sync(entry.getValue());
        }
    }


    public long getChecksumOfLocalFile(FileMetadata fileMetadata) throws IOException {
        logger.debug("Getting checksum of local file: {}", fileMetadata.file());
        return calculateChecksum(fileMetadata);
    }

    /**
     * Returns the byte length of a file using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @return the length of the file in bytes
     * @throws IOException if the file cannot be accessed
     */
    public long fileLength(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.fileLength(fileMetadata.file());
    }

    /**
     * Deletes a file using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @throws IOException if the file exists but could not be deleted
     */
    public void deleteFile(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
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
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
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
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
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
        FormatStoreDirectory<?> targetDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
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
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.calculateChecksum(fileMetadata.file());
    }

    /**
     * Calculates upload checksum using FileMetadata for format routing
     * @param fileMetadata the FileMetadata containing format and filename information
     * @return checksum string in format-specific representation
     * @throws IOException if checksum calculation fails
     */
    public String calculateUploadChecksum(FileMetadata fileMetadata) throws IOException {
        FormatStoreDirectory<?> formatDirectory = getDirectoryForFormat(fileMetadata.dataFormat());
        return formatDirectory.calculateUploadChecksum(fileMetadata.file());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegates);
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
