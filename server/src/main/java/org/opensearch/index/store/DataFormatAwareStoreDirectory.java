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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.index.store.checksum.LuceneChecksumHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Format-aware directory that extends {@link FilterDirectory} and wraps a {@link SubdirectoryAwareDirectory}.
 *
 * <p>This directory adds data format awareness on top of the subdirectory path routing
 * provided by {@link SubdirectoryAwareDirectory}. It understands that files in different
 * subdirectories may belong to different data formats (Lucene, Parquet, Arrow, etc.) and
 * provides format-specific operations, most notably checksum calculation.</p>
 *
 * <p><strong>Delegated to SubdirectoryAwareDirectory:</strong></p>
 * <ul>
 *   <li>Path routing: plain filenames → index/, prefixed filenames → subdirectories</li>
 *   <li>File operations: openInput, createOutput, deleteFile, fileLength, listAll, rename, sync</li>
 * </ul>
 *
 * <p><strong>Added by DataFormatAwareStoreDirectory:</strong></p>
 * <ul>
 *   <li>FileMetadata support: parse file identifier strings into FileMetadata objects</li>
 *   <li>Format-aware checksum: Lucene files → CodecUtil footer, others → CRC32 full-file</li>
 *   <li>Dual API: callers can use String or FileMetadata for all operations</li>
 * </ul>
 *
 * <p><strong>File naming convention:</strong></p>
 * <pre>
 *   Lucene:   "_0.cfs"                → stored in &lt;shard&gt;/index/_0.cfs
 *   Parquet:  "parquet/_0_1.parquet"  → stored in &lt;shard&gt;/parquet/_0_1.parquet
 *   Arrow:    "arrow/_0_1.arrow"      → stored in &lt;shard&gt;/arrow/_0_1.arrow
 * </pre>
 *
 * <p><strong>Checksum strategy:</strong></p>
 * <ul>
 *   <li>Lucene/index files: {@code CodecUtil.retrieveChecksum()} — reads checksum from codec footer (fast, O(1))</li>
 *   <li>Non-Lucene files: Full-file CRC32 scan — computes CRC32 over all bytes (generic, O(n))</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class DataFormatAwareStoreDirectory extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(DataFormatAwareStoreDirectory.class);

    private static final String DEFAULT_FORMAT = "lucene";

    private static final Set<String> INDEX_DIRECTORY_FORMATS = Set.of("lucene", "metadata");

    private final ShardPath shardPath;
    private final Map<String, FormatChecksumStrategy> checksumStrategies;
    private static final FormatChecksumStrategy DEFAULT_CHECKSUM_STRATEGY = new GenericCRC32ChecksumHandler();

    /**
     * Constructs a DataFormatAwareStoreDirectory with pre-built checksum strategies for
     * format-aware checksum calculation and other format-specific operations.
     *
     * @param delegate            the underlying FSDirectory (typically for &lt;shard&gt;/index/)
     * @param shardPath           the shard path for resolving subdirectories
     * @param checksumStrategies  pre-built checksum strategies keyed by format name
     */
    public DataFormatAwareStoreDirectory(Directory delegate, ShardPath shardPath, Map<String, FormatChecksumStrategy> checksumStrategies) {
        super(new SubdirectoryAwareDirectory(delegate, shardPath));
        this.shardPath = shardPath;
        this.checksumStrategies = new HashMap<>(checksumStrategies);
        this.checksumStrategies.put(DEFAULT_FORMAT, new LuceneChecksumHandler());
        logger.debug(
            "Created DataFormatAwareStoreDirectory for shard {} with checksum strategies for formats: {}",
            shardPath.getShardId(),
            this.checksumStrategies.keySet()
        );
    }

    /**
     * Walks the {@link FilterDirectory} wrapping chain to find a {@link DataFormatAwareStoreDirectory}.
     * This is needed because the directory may be wrapped in {@link ByteSizeCachingDirectory} and
     * {@link Store.StoreDirectory}, so a direct {@code instanceof} check on the outermost directory
     * would fail.
     *
     * @param dir the directory to unwrap (may be null)
     * @return the DataFormatAwareStoreDirectory found in the chain, or null if not present
     */
    public static DataFormatAwareStoreDirectory unwrap(Directory dir) {
        while (dir != null) {
            if (dir instanceof DataFormatAwareStoreDirectory) {
                return (DataFormatAwareStoreDirectory) dir;
            }
            if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                return null;
            }
        }
        return null;
    }

    private String resolveFileName(String fileName) {
        if (fileName.contains(FileMetadata.DELIMITER)) {
            FileMetadata fm = new FileMetadata(fileName);
            fileName = toFileIdentifier(fm);
        }
        return fileName;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return in.openInput(resolveFileName(name), context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return in.createOutput(resolveFileName(name), context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        in.deleteFile(resolveFileName(name));
    }

    @Override
    public long fileLength(String name) throws IOException {
        return in.fileLength(resolveFileName(name));
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        in.sync(names.stream().map(this::resolveFileName).collect(Collectors.toList()));
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        in.rename(resolveFileName(source), resolveFileName(dest));
    }

    @Override
    public String[] listAll() throws IOException {
        String[] allFiles = in.listAll();
        for (int i = 0; i < allFiles.length; i++) {
            // Normalize OS-dependent separators (e.g., "\" on Windows) to "/" before parsing,
            // since SubdirectoryAwareDirectory.listAll() returns Path.toString() which uses
            // the OS separator, but FileMetadata expects "/" as the format/file delimiter.
            String normalized = allFiles[i].replace(
                org.opensearch.common.io.PathUtils.getDefaultFileSystem().getSeparator().charAt(0),
                '/'
            );
            FileMetadata fm = toFileMetadata(normalized);
            allFiles[i] = isDefaultFormat(fm.dataFormat()) ? fm.file() : fm.serialize();
        }
        return allFiles;
    }

    // ═══════════════════════════════════════════════════════════════
    // FileMetadata parsing and conversion
    // ═══════════════════════════════════════════════════════════════

    /**
     * Parses a file identifier string into a {@link FileMetadata} object.
     * Uses the same "format/file" convention as {@link FileMetadata} (e.g., "parquet/_0.pqt").
     * Plain filenames without a "/" prefix default to the lucene format.
     *
     * @param fileIdentifier the file path string (with optional format prefix separated by '/')
     * @return FileMetadata with parsed dataFormat and filename
     */
    public static FileMetadata toFileMetadata(String fileIdentifier) {
        return new FileMetadata(fileIdentifier);
    }

    /**
     * Converts a {@link FileMetadata} object back to a file identifier string.
     *
     * @param fm the FileMetadata to convert
     * @return file identifier string suitable for Directory operations
     */
    public static String toFileIdentifier(FileMetadata fm) {
        String format = fm.dataFormat();
        if (isDefaultFormat(format)) {
            return fm.file();
        }
        return format + "/" + fm.file();
    }

    // ═══════════════════════════════════════════════════════════════
    // Format-Aware Checksum Calculation
    // ═══════════════════════════════════════════════════════════════

    public long calculateChecksum(String name) throws IOException {
        FileMetadata fm = toFileMetadata(name);
        return calculateChecksum(fm);
    }

    /**
     * Calculates checksum using the format-specific {@link FormatChecksumStrategy}.
     * Supports pre-computed checksums (O(1) for formats that register them during write)
     * and falls back to file-based computation for formats that don't.
     */
    private long calculateChecksum(FileMetadata fm) throws IOException {
        String fileIdentifier = toFileIdentifier(fm);
        FormatChecksumStrategy strategy = checksumStrategies.getOrDefault(fm.dataFormat(), DEFAULT_CHECKSUM_STRATEGY);
        return strategy.computeChecksum(this, fileIdentifier);
    }

    /**
     * Calculates a checksum suitable for upload verification.
     * Public API used by RemoteSegmentStoreDirectory.
     */
    public String calculateUploadChecksum(String name) throws IOException {
        return Long.toString(calculateChecksum(name));
    }

    /**
     * Returns the checksum strategy for the given format, or {@code null} if none is registered.
     * Engines use this to share the directory's strategy instance so that pre-computed
     * checksums registered during write are visible to the upload path.
     *
     * @param format the data format name (e.g., "parquet")
     * @return the strategy, or null if not found
     */
    public FormatChecksumStrategy getChecksumStrategy(String format) {
        return checksumStrategies.get(format);
    }

    public IndexOutput createOutput(FileMetadata fm, IOContext context) throws IOException {
        return createOutput(toFileIdentifier(fm), context);
    }

    public String getDataFormat(String fileIdentifier) {
        return toFileMetadata(fileIdentifier).dataFormat();
    }

    public ShardPath getShardPath() {
        return shardPath;
    }

    // ═══════════════════════════════════════════════════════════════
    // Private Helpers
    // ═══════════════════════════════════════════════════════════════

    private static boolean isDefaultFormat(String format) {
        return format == null || format.isEmpty() || INDEX_DIRECTORY_FORMATS.contains(format.toLowerCase(Locale.ROOT));
    }
}
