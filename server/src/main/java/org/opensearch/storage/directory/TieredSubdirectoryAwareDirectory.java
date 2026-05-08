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
import org.apache.lucene.index.IndexFileNames;
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
import org.opensearch.storage.indexinput.FormatSwitchableIndexInput;
import org.opensearch.storage.indexinput.FormatSwitchableIndexInputWrapper;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    /** Per-file original FormatSwitchableIndexInput — used by afterSyncToRemote to switch before local delete. */
    private final ConcurrentMap<String, FormatSwitchableIndexInput> formatInputs = new ConcurrentHashMap<>();

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
        ensureOpen();
        if (isFormatFile(name)) {
            // File already synced to remote — read directly from remote.
            if (remoteDirectory.getExistingRemoteFilename(name) != null) {
                return remoteDirectory.openInput(name, context);
            }
            // File not yet synced — wrap in FormatSwitchableIndexInput so that if
            // afterSyncToRemote deletes the local copy mid-read, the reader switches
            // to remote transparently (same pattern as SwitchableIndexInput for Lucene files).
            IndexInput localInput = in.openInput(name, context);
            FormatSwitchableIndexInput switchable = new FormatSwitchableIndexInput(
                "FormatSwitchable(" + name + ")",
                name,
                localInput,
                remoteDirectory
            );
            formatInputs.put(name, switchable);
            return new FormatSwitchableIndexInputWrapper("FormatSwitchable(" + name + ")", switchable);
        }
        // Lucene files: if it's a segments_N file not in remote metadata, try local disk first.
        // Handles restart where segments_N was written locally but has a different generation than remote.
        if (name.startsWith(IndexFileNames.SEGMENTS) && remoteDirectory.getExistingRemoteFilename(name) == null) {
            try {
                return in.openInput(name, context);
            } catch (NoSuchFileException e) {
                // Not on local disk either — fall through to TieredDirectory
            }
        }
        return tieredDirectory.openInput(name, context);
    }

    @Override
    public long fileLength(String name) throws IOException {
        if (isFormatFile(name)) {
            // Same routing as openInput — check remote first.
            if (remoteDirectory.getExistingRemoteFilename(name) != null) {
                return remoteDirectory.fileLength(name);
            }
            return in.fileLength(name);
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
        // Format files (subdirectory) need SubdirectoryAwareDirectory which creates parent dirs.
        // CompositeDirectory doesn't handle subdirectory path creation.
        if (isFormatFile(name)) {
            return in.createOutput(name, context);
        }
        return tieredDirectory.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        if (isFormatFile(name)) {
            String blobKey = remoteDirectory.getExistingRemoteFilename(name);
            if (blobKey == null) {
                strategies.onRemoved(name);
            }
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
            StoreStrategyRegistry.Match match = strategies.matchFor(file);
            String format = match != null ? match.format().name() : "";
            strategies.onUploaded(file, remoteDirectory.getRemoteBasePath(format), blobKey, size);

            // Switch any in-flight reader to remote before deleting local.
            // The switchToRemote cascades to all clones/slices via the shared lock.
            FormatSwitchableIndexInput switchable = formatInputs.remove(file);
            if (switchable != null) {
                try {
                    switchable.switchToRemote();
                } catch (IOException e) {
                    logger.warn("afterSyncToRemote: failed to switch to remote for file={}", file, e);
                }
            }

            // Now safe to delete local — all readers are on remote.
            try {
                in.deleteFile(file);
            } catch (java.nio.file.NoSuchFileException e) {
                // Already gone — fine
            } catch (IOException e) {
                logger.warn("afterSyncToRemote: failed to delete local copy of file={}", file);
            }
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
        // Format files may be renamed during recovery (recovery.{uuid}.filename → filename).
        // Route through SubdirectoryAwareDirectory which handles subdirectory paths.
        if (isFormatFile(source)) {
            in.rename(source, dest);
            return;
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
