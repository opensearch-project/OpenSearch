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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.RemoteSyncListener;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A tiered directory that combines subdirectory-aware local storage with remote tiered storage
 * and per-format directory routing.
 *
 * <p>This directory extends {@link FilterDirectory} wrapping a {@link SubdirectoryAwareDirectory}
 * and implements {@link RemoteSyncListener} for remote sync notifications. It routes file
 * operations based on data format:
 * <ul>
 *   <li>Files with a format-specific directory (e.g., parquet) are routed to that directory</li>
 *   <li>Files without a format directory (e.g., Lucene) are routed to the internal {@link TieredDirectory}</li>
 * </ul>
 *
 * <p>The directory stack for warm+format nodes:
 * <pre>
 *   DataFormatAwareStoreDirectory (checksums, format metadata)
 *     → TieredSubdirectoryAwareDirectory (this class — format routing)
 *       ├── wraps: SubdirectoryAwareDirectory → FSDirectory (local path routing)
 *       ├── holds: TieredDirectory(SubdirectoryAwareDirectory, RemoteDir, FileCache, ThreadPool)
 *       └── holds: Map&lt;String, Directory&gt; (per-format directories from DataFormatDescriptor)
 * </pre>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredSubdirectoryAwareDirectory extends FilterDirectory implements RemoteSyncListener {

    private static final Logger logger = LogManager.getLogger(TieredSubdirectoryAwareDirectory.class);

    private final TieredDirectory tieredDirectory;
    private final Map<String, Directory> formatDirectories;

    /**
     * Constructs a TieredSubdirectoryAwareDirectory.
     *
     * @param localDirectory    the subdirectory-aware local directory (used as FilterDirectory delegate)
     * @param remoteDirectory   the remote segment store directory
     * @param fileCache         the file cache for warm node caching
     * @param threadPool        the thread pool for async operations
     * @param formatDirectories per-format directories (format name → directory), from DataFormatDescriptor
     * @param tieredStoragePrefetchSettingsSupplier supplier for prefetch settings
     */
    public TieredSubdirectoryAwareDirectory(
        SubdirectoryAwareDirectory localDirectory,
        Directory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        Map<String, Directory> formatDirectories,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(localDirectory);
        this.formatDirectories = formatDirectories;
        boolean success = false;
        try {
            this.tieredDirectory = new TieredDirectory(
                localDirectory,
                remoteDirectory,
                fileCache,
                threadPool,
                tieredStoragePrefetchSettingsSupplier
            );
            logger.debug("Created TieredSubdirectoryAwareDirectory with format directories: {}", formatDirectories.keySet());
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(formatDirectories.values());
            }
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        Directory formatDir = resolveFormatDirectory(name);
        if (formatDir != null) {
            return formatDir.openInput(name, context);
        }
        return tieredDirectory.openInput(name, context);
    }

    @Override
    public long fileLength(String name) throws IOException {
        Directory formatDir = resolveFormatDirectory(name);
        if (formatDir != null) {
            return formatDir.fileLength(name);
        }
        return tieredDirectory.fileLength(name);
    }

    @Override
    public String[] listAll() throws IOException {
        Set<String> all = new HashSet<>(Arrays.asList(tieredDirectory.listAll()));
        for (Directory formatDir : formatDirectories.values()) {
            Collections.addAll(all, formatDir.listAll());
        }
        return all.stream().sorted().toArray(String[]::new);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        Directory formatDir = resolveFormatDirectory(name);
        if (formatDir != null) {
            return formatDir.createOutput(name, context);
        }
        return tieredDirectory.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        Directory formatDir = resolveFormatDirectory(name);
        if (formatDir != null) {
            formatDir.deleteFile(name);
        } else {
            tieredDirectory.deleteFile(name);
        }
    }

    @Override
    public void afterSyncToRemote(String file) {
        Directory formatDir = resolveFormatDirectory(file);
        if (formatDir != null) {
            if (formatDir instanceof RemoteSyncListener) {
                ((RemoteSyncListener) formatDir).afterSyncToRemote(file);
            }
            // else: format directory doesn't support sync notifications — no-op
        } else {
            tieredDirectory.afterSyncToRemote(file);
        }
    }

    @Override
    public void close() throws IOException {
        List<Closeable> toClose = new ArrayList<>(formatDirectories.values());
        toClose.add(tieredDirectory);
        IOUtils.close(toClose);
    }

    /**
     * Resolves the format-specific directory for the given file name by parsing the data format.
     *
     * @param name the file name or identifier
     * @return the directory for the file's format, or {@code null} if no format directory is registered
     */
    @SuppressWarnings("resource")
    private Directory resolveFormatDirectory(String name) {
        String format = DataFormatAwareStoreDirectory.toFileMetadata(name).dataFormat();
        return formatDirectories.get(format);
    }
}
