/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DataFormatAwareStoreHandler;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSyncListener;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A tiered directory for warm nodes that routes file operations based on data format.
 *
 * <p><b>Read-only warm (current scope):</b> all format files are REMOTE — seeded from
 * remote metadata at shard open. Reads go directly to {@link RemoteSegmentStoreDirectory}.
 * No local copies, no eviction, no ref counting for format files.
 *
 * <p><b>Routing:</b>
 * <ul>
 *   <li>Format files (handler found) → always {@link RemoteSegmentStoreDirectory}</li>
 *   <li>Lucene files (no handler) → {@link TieredDirectory} (FileCache + remote)</li>
 * </ul>
 *
 * <p><b>For format plugin implementors (future writable warm):</b> when LOCAL files exist,
 * {@code openInput} will need {@code acquireRead}/{@code releaseRead} ref counting to prevent
 * eviction while a reader holds the file open. See TODO comments in {@link #openInput} and
 * {@link #fileLength}. The Rust FileRegistry ref counting is the reference implementation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredSubdirectoryAwareDirectory extends FilterDirectory implements RemoteSyncListener {

    private static final Logger logger = LogManager.getLogger(TieredSubdirectoryAwareDirectory.class);

    private final TieredDirectory tieredDirectory;
    private final Map<String, DataFormatAwareStoreHandler> formatStoreHandlers;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final ShardPath shardPath;

    /**
     * Constructs a TieredSubdirectoryAwareDirectory.
     *
     * @param localDirectory    the subdirectory-aware local directory
     * @param remoteDirectory   the remote segment store directory
     * @param fileCache         the file cache for warm node caching
     * @param threadPool        the thread pool for async operations
     * @param formatStoreHandlers per-format store handlers (format name → handler), or empty map
     * @param shardPath         the shard path for resolving index directory
     * @param tieredStoragePrefetchSettingsSupplier supplier for prefetch settings
     */
    public TieredSubdirectoryAwareDirectory(
        SubdirectoryAwareDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Map<String, DataFormatAwareStoreHandler> formatStoreHandlers,
        ShardPath shardPath,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(localDirectory);
        this.formatStoreHandlers = formatStoreHandlers;
        this.remoteDirectory = remoteDirectory;
        this.shardPath = shardPath;
        boolean success = false;
        try {
            this.tieredDirectory = new TieredDirectory(
                localDirectory,
                remoteDirectory,
                fileCache,
                threadPool,
                tieredStoragePrefetchSettingsSupplier
            );
            logger.debug("Created TieredSubdirectoryAwareDirectory with format handlers: {}", formatStoreHandlers.keySet());
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(formatStoreHandlers.values());
            }
        }
    }

    /**
     * Opens an IndexInput for reading a file.
     *
     * <p>Format files → remote store (all format files are REMOTE on read-only warm).
     * Lucene files → {@link TieredDirectory} (FileCache + remote block reads).
     *
     * <p>TODO (writable warm): when LOCAL format files exist, add acquireRead/releaseRead
     * ref counting here to prevent eviction while a reader holds the file open. Pattern:
     * {@code handler.acquireRead(name)} → open local → wrap in RefCountedIndexInput →
     * {@code releaseRead} on close. See Rust FileRegistry ReadGuard for reference.
     */
    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        DataFormatAwareStoreHandler handler = resolveHandler(name);
        if (handler != null) {
            // Read-only warm: all format files are REMOTE, read from remote store.
            // Need until checksum issue is sorted
            return remoteDirectory.openInput(name, context);
        }
        return tieredDirectory.openInput(name, context);
    }

    /**
     * Returns the byte length of a file.
     *
     * <p>Format files → remote store metadata (in-memory map, no network call).
     * Lucene files → {@link TieredDirectory}.
     *
     * <p>No ref counting needed — this is an atomic metadata lookup. If the file is
     * deleted between the handler check and the actual call, callers handle
     * {@link java.nio.file.NoSuchFileException} (e.g., ByteSizeCachingDirectory, Engine.commitStats).
     *
     * <p>TODO (writable warm): when LOCAL format files exist, route LOCAL → localDirectory,
     * REMOTE → remoteDirectory based on handler.getFileLocation().
     */
    @Override
    public long fileLength(String name) throws IOException {
        DataFormatAwareStoreHandler handler = resolveHandler(name);
        if (handler != null) {
            return remoteDirectory.fileLength(name);
        }
        return tieredDirectory.fileLength(name);
    }

    /**
     * Lists all files visible to this directory.
     *
     * <p>Only returns Lucene files from {@link TieredDirectory}. Format files tracked
     * by handlers are not included — they are accessed via handler routing, not listed.
     *
     * <p>TODO (writable warm): merge handler-tracked files into the listing if needed
     * for recovery or stats.
     */
    @Override
    public String[] listAll() throws IOException {
        Set<String> all = new HashSet<>(Arrays.asList(tieredDirectory.listAll()));
        return all.stream().sorted().toArray(String[]::new);
    }

    /**
     * Creates an output for writing a new file. Always delegates to {@link TieredDirectory}.
     * Format files are written by native writers (Rust), not through the Directory API.
     */
    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return tieredDirectory.createOutput(name, context);
    }

    /**
     * Deletes a file.
     *
     * <p><b>Format files:</b> removes from handler tracking, then attempts to delete the local
     * copy via the underlying {@link SubdirectoryAwareDirectory} (ignores {@link NoSuchFileException}
     * if the file is remote-only or already evicted).
     * <p><b>Lucene files:</b> delegates to {@link TieredDirectory}.
     */
    @Override
    public void deleteFile(String name) throws IOException {
        DataFormatAwareStoreHandler handler = resolveHandler(name);
        if (handler != null) {
            handler.removeFile(name);
            // Also delete local copy if it exists (file may be remote-only on read-only warm)
            try {
                in.deleteFile(name);
            } catch (NoSuchFileException e) {
                // Expected on read-only warm — file was never local or already evicted
            }
            return;
        }
        tieredDirectory.deleteFile(name);
    }

    @Override
    public void afterSyncToRemote(String file) {
        DataFormatAwareStoreHandler handler = resolveHandler(file);
        if (handler != null) {
            String blobKey = remoteDirectory.getExistingRemoteFilename(file);
            String format = DataFormatAwareStoreDirectory.toFileMetadata(file).dataFormat();
            String basePath = remoteDirectory.getRemoteBasePath();
            String remotePath = basePath + ((format != null && format.isEmpty() == false) ? format + "/" : "") + blobKey;
            handler.afterSyncToRemote(file, remotePath);
            return;
        }
        tieredDirectory.afterSyncToRemote(file);
    }

    @Override
    public void close() throws IOException {
        List<Closeable> toClose = new ArrayList<>(formatStoreHandlers.values());
        toClose.add(tieredDirectory);
        IOUtils.close(toClose);
    }

    /**
     * Resolves the format store handler for the given file name.
     *
     * <p>Returns the handler for the file's format, or {@code null} if the file is a plain
     * Lucene/metadata file (no subdirectory prefix) handled by {@link TieredDirectory}.
     *
     * <p>A file with a subdirectory prefix (contains "/", e.g., "parquet/seg.parquet") is a
     * format file and MUST have a registered handler. If no handler is found, this indicates
     * a misconfiguration or missing plugin.
     *
     * @throws IllegalStateException if the file has a subdirectory prefix but no handler is registered
     */
    private DataFormatAwareStoreHandler resolveHandler(String name) {
        // Files that resolve to shardPath.resolveIndex() (no parent directory) are Lucene/metadata
        // files handled by TieredDirectory — no handler needed.
        if (shardPath.resolveIndex().resolve(name).getParent().equals(shardPath.resolveIndex())) {
            return null;
        }
        String format = DataFormatAwareStoreDirectory.toFileMetadata(name).dataFormat();
        DataFormatAwareStoreHandler handler = formatStoreHandlers.get(format);
        if (handler == null) {
            throw new IllegalStateException(
                "No DataFormatAwareStoreHandler registered for format ["
                    + format
                    + "] — file ["
                    + name
                    + "]. Ensure the format plugin is installed and provides a handler."
            );
        }
        return handler;
    }
}
