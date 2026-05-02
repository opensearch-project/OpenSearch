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
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSyncListener;
import org.opensearch.index.store.SubdirectoryAwareDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A tiered directory for warm nodes that routes file operations based on
 * data format.
 *
 * <p><b>Read-only warm (current scope):</b> all format files are REMOTE,
 * seeded from remote metadata at shard open via {@link StoreStrategyRegistry}.
 * Reads go directly to {@link RemoteSegmentStoreDirectory}. No local copies,
 * no eviction, no ref counting for format files.
 *
 * <p><b>Routing:</b>
 * <ul>
 *   <li>Format files (a strategy claims the file) → always
 *       {@link RemoteSegmentStoreDirectory}</li>
 *   <li>Lucene files (no claiming strategy) → {@link TieredDirectory}
 *       (FileCache + remote)</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class TieredSubdirectoryAwareDirectory extends FilterDirectory implements RemoteSyncListener {

    private static final Logger logger = LogManager.getLogger(TieredSubdirectoryAwareDirectory.class);

    private final TieredDirectory tieredDirectory;
    private final StoreStrategyRegistry strategies;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final ShardPath shardPath;

    public TieredSubdirectoryAwareDirectory(
        SubdirectoryAwareDirectory localDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        FileCache fileCache,
        ThreadPool threadPool,
        StoreStrategyRegistry strategies,
        ShardPath shardPath,
        Supplier<TieredStoragePrefetchSettings> tieredStoragePrefetchSettingsSupplier
    ) {
        super(localDirectory);
        this.strategies = strategies == null ? StoreStrategyRegistry.EMPTY : strategies;
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
            logger.debug("Created TieredSubdirectoryAwareDirectory (hasStoreHandlers={})", this.strategies.hasStoreHandlers());
            success = true;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(this.strategies);
            }
        }
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        if (isFormatFile(name)) {
            // TODO (writable warm): when LOCAL format files exist, add acquireRead/releaseRead
            // ref counting. Pattern: acquireRead → localDirectory.openInput → wrap in
            // RefCountedIndexInput → releaseRead on close. Currently all format files are REMOTE.
            return remoteDirectory.openInput(name, context);
        }
        return tieredDirectory.openInput(name, context);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (isFormatFile(name)) {
            // TODO (writable warm): route LOCAL → localDirectory, REMOTE → remoteDirectory
            // based on registry location. Currently all format files are REMOTE.
            return remoteDirectory.fileLength(name);
        }
        return tieredDirectory.fileLength(name);
    }

    @Override
    public String[] listAll() throws IOException {
        Set<String> all = new HashSet<>(Arrays.asList(tieredDirectory.listAll()));
        return all.stream().sorted().toArray(String[]::new);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return tieredDirectory.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (isFormatFile(name)) {
            strategies.onRemoved(name);
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
        if (isFormatFile(file)) {
            String blobKey = remoteDirectory.getExistingRemoteFilename(file);
            if (blobKey == null) {
                throw new IllegalStateException(
                    "afterSyncToRemote called for format file [" + file + "] but no remote filename found in metadata"
                );
            }
            long size;
            try {
                size = remoteDirectory.fileLength(file);
            } catch (IOException e) {
                size = 0;
            }
            strategies.onUploaded(file, remoteDirectory.getRemoteBasePath(), blobKey, size);
            return;
        }
        tieredDirectory.afterSyncToRemote(file);
    }

    @Override
    public void sync(Collection<String> names) {
        // Skip — same as TieredDirectory (CompositeDirectory). On warm, files are
        // either remote-only (format files) or cached from remote.
        // No local writes to fsync. Writable warm will need to revisit this.
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        // Rename is only called by Lucene's IndexWriter during commit
        // (pending_segments_N → segments_N). Format files are never renamed.
        if (isFormatFile(source)) {
            throw new IllegalStateException("Rename not supported for format file [" + source + "]. Format files are write-once.");
        }
        tieredDirectory.rename(source, dest);
    }

    @Override
    public void close() throws IOException {
        // Native registries close before the directory so native resources are
        // torn down while the Java resources they may reference are still alive.
        IOUtils.close(strategies, tieredDirectory);
    }

    /**
     * Returns {@code true} if {@code name} is a format file (claimed by a
     * registered {@link StoreStrategy}). Plain Lucene/metadata files — those
     * whose path resolves directly under the shard index directory — are not
     * format files and skip the strategy lookup.
     *
     * <p>The {@code shardPath.resolveIndex()} guard is a fast-path: files without
     * a subdirectory component (e.g. {@code "_0.cfe"}) are always Lucene files.
     * Only files under a subdirectory (e.g. {@code "parquet/seg_0.parquet"}) go
     * through the strategy lookup via {@link StoreStrategyRegistry#matchFor}.
     */
    private boolean isFormatFile(String name) {
        if (shardPath.resolveIndex().resolve(name).getParent().equals(shardPath.resolveIndex())) {
            return false;
        }
        StoreStrategyRegistry.Match match = strategies.matchFor(name);
        if (match == null) {
            throw new IllegalStateException("No StoreStrategy registered for file [" + name + "]. Ensure the format plugin is installed.");
        }
        return true;
    }
}
