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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandler;
import org.opensearch.index.engine.dataformat.DataFormatStoreHandlerFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.repositories.NativeStoreRepository;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-shard registry of {@link StoreStrategy} instances and their associated
 * {@link DataFormatStoreHandler store handlers}.
 *
 * <p>Owns the plumbing shared by every data format participating in the tiered
 * store so that format plugins stay purely declarative:
 * <ul>
 *   <li>resolves the owning {@link StoreStrategy} for a file</li>
 *   <li>constructs per-strategy {@link DataFormatStoreHandler} instances
 *       exception-safely (no leaked native resources if one factory throws)</li>
 *   <li>seeds handlers from the remote segment metadata at open time</li>
 *   <li>forwards {@code onUploaded} / {@code onRemoved} events to the owning
 *       strategy's handler, if any</li>
 *   <li>closes handlers in the right order when the shard shuts down</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class StoreStrategyRegistry implements Closeable {

    private static final Logger logger = LogManager.getLogger(StoreStrategyRegistry.class);

    /** Sentinel for "no strategies registered on this shard". Safe to close. */
    public static final StoreStrategyRegistry EMPTY = new StoreStrategyRegistry(null, Collections.emptyMap(), Collections.emptyMap());

    /** Shard path for resolving absolute file keys (matches DataFusion's lookup paths). Null for EMPTY. */
    private final ShardPath shardPath;
    /** Strategies keyed by data format. */
    private final Map<DataFormat, StoreStrategy> strategies;
    /** Store handlers keyed by data format. Absent for strategies without one. */
    private final Map<DataFormat, DataFormatStoreHandler> storeHandlers;

    /**
     * A strategy paired with the data format it is registered under. Used
     * internally for routing decisions so callers never need to re-derive the
     * format from the strategy.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public record Match(DataFormat format, StoreStrategy strategy) {
    }

    private StoreStrategyRegistry(
        ShardPath shardPath,
        Map<DataFormat, StoreStrategy> strategies,
        Map<DataFormat, DataFormatStoreHandler> storeHandlers
    ) {
        this.shardPath = shardPath;
        this.strategies = Map.copyOf(strategies);
        this.storeHandlers = Map.copyOf(storeHandlers);
    }

    /**
     * Builds a registry for a shard, constructing per-strategy store handlers
     * and seeding them from the remote metadata.
     *
     * <p>If any handler factory throws, all handlers created so far
     * are closed and the exception is rethrown — no partial state escapes.
     *
     * @param shardPath       the shard path (used to resolve absolute file paths for DataFusion)
     * @param isWarm          true on warm nodes
     * @param nativeStore     the repository's native store, or
     *                        {@link NativeStoreRepository#EMPTY}
     * @param strategies      the strategies that apply to this shard, keyed by data format
     * @param remoteDirectory the remote segment store directory used to seed initial state
     * @return a fully-initialised registry
     */
    public static StoreStrategyRegistry open(
        ShardPath shardPath,
        boolean isWarm,
        NativeStoreRepository nativeStore,
        Map<DataFormat, StoreStrategy> strategies,
        RemoteSegmentStoreDirectory remoteDirectory
    ) {
        if (strategies == null || strategies.isEmpty()) {
            return EMPTY;
        }

        // Exception safety: if any factory throws, all previously created handlers
        // are closed in the finally block. This prevents native resource leaks when
        // one format plugin fails during shard open.
        Map<DataFormat, DataFormatStoreHandler> storeHandlers = new HashMap<>();
        List<DataFormatStoreHandler> created = new ArrayList<>();
        boolean success = false;
        try {
            for (Map.Entry<DataFormat, StoreStrategy> entry : strategies.entrySet()) {
                DataFormat format = entry.getKey();
                StoreStrategy strategy = entry.getValue();
                DataFormatStoreHandlerFactory factory = strategy.storeHandler().orElse(null);
                if (factory == null) {
                    continue;
                }
                DataFormatStoreHandler handler = factory.create(shardPath.getShardId(), isWarm, nativeStore);
                if (handler != null) {
                    storeHandlers.put(format, handler);
                    created.add(handler);
                }
            }

            if (storeHandlers.isEmpty() == false) {
                seedFromRemoteMetadata(shardPath, strategies, storeHandlers, remoteDirectory);
            }
            success = true;
            return new StoreStrategyRegistry(shardPath, strategies, storeHandlers);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(created);
            }
        }
    }

    /**
     * Returns the strategy that owns {@code file}, or {@code null} if no
     * registered strategy claims it. The returned {@link Match} carries both
     * the data format and the strategy object.
     */
    public Match matchFor(String file) {
        if (file == null) {
            return null;
        }
        for (Map.Entry<DataFormat, StoreStrategy> entry : strategies.entrySet()) {
            String name = entry.getKey().name();
            if (entry.getValue().owns(name, file)) {
                return new Match(entry.getKey(), entry.getValue());
            }
        }
        return null;
    }

    /** True if any strategy on this shard has a store handler. */
    public boolean hasStoreHandlers() {
        return storeHandlers.isEmpty() == false;
    }

    /**
     * Returns the native store handles for all formats that have a live handler,
     * keyed by {@link DataFormat}.
     *
     * <p>The reader manager uses this to register native object stores in the
     * DataFusion runtime environment.
     *
     * @return map of DataFormat to live {@link NativeStoreHandle}, or empty if
     *         no handlers have native stores
     */
    public Map<DataFormat, NativeStoreHandle> getFormatStoreHandles() {
        Map<DataFormat, NativeStoreHandle> handles = new HashMap<>();
        for (Map.Entry<DataFormat, DataFormatStoreHandler> entry : storeHandlers.entrySet()) {
            NativeStoreHandle handle = entry.getValue().getFormatStoreHandle();
            if (handle != null && handle.isLive()) {
                handles.put(entry.getKey(), handle);
            }
        }
        return Map.copyOf(handles);
    }

    /**
     * Forwards a sync-to-remote event. Resolves the owning strategy, constructs
     * the remote path via {@link StoreStrategy#remotePath}, and forwards to the
     * store handler for that strategy if one exists.
     *
     * @param file            the file identifier that was uploaded
     * @param basePath        the repository base path
     * @param uploadedBlobKey the blob key assigned by the upload path
     * @return true if the event was dispatched to a store handler; false if
     *         no strategy owns the file or the owning strategy has no handler
     */
    public boolean onUploaded(String file, String basePath, String uploadedBlobKey, long size) {
        Match match = matchFor(file);
        if (match == null) {
            return false;
        }
        DataFormatStoreHandler handler = storeHandlers.get(match.format());
        if (handler == null) {
            return false;
        }
        // Resolve absolute key for the native handler's Rust registry (matches DataFusion lookups)
        String absoluteKey = shardPath.getDataPath().resolve(file).toString();
        String remotePath = match.strategy().remotePath(match.format().name(), basePath, file, uploadedBlobKey);
        handler.onUploaded(absoluteKey, remotePath, size);
        return true;
    }

    /**
     * Forwards a removal event. Returns true if dispatched, false otherwise.
     */
    public boolean onRemoved(String file) {
        Match match = matchFor(file);
        if (match == null) {
            return false;
        }
        DataFormatStoreHandler handler = storeHandlers.get(match.format());
        if (handler == null) {
            return false;
        }
        // Resolve absolute key for the native handler's Rust registry
        String absoluteKey = shardPath.getDataPath().resolve(file).toString();
        handler.onRemoved(absoluteKey);
        return true;
    }

    /**
     * Closes all store handlers. Handlers are closed before the directory
     * (in {@link TieredSubdirectoryAwareDirectory#close}) so Rust resources
     * are torn down while the Java objects they may reference are still alive.
     */
    @Override
    public void close() throws IOException {
        IOUtils.close(storeHandlers.values());
    }

    // TODO (writable warm): add seedLocalFiles(ShardPath) — scan local disk at shard open
    // for crash recovery. Registers LOCAL files that were written but not yet synced to remote.

    /**
     * Seeds store handlers from the remote segment store metadata.
     * Called once at shard open. Each file is matched to its owning strategy,
     * the remote blob path is constructed, and the batch is forwarded to the
     * strategy's store handler.
     *
     * <p>Currently seeds all files as REMOTE. On writable warm, local files
     * from a disk scan would be seeded as LOCAL via a separate path.
     */
    private static void seedFromRemoteMetadata(
        ShardPath shardPath,
        Map<DataFormat, StoreStrategy> strategies,
        Map<DataFormat, DataFormatStoreHandler> storeHandlers,
        RemoteSegmentStoreDirectory remoteDirectory
    ) {
        if (remoteDirectory == null) {
            return;
        }
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploaded = remoteDirectory.getSegmentsUploadedToRemoteStore();
        if (uploaded == null || uploaded.isEmpty()) {
            return;
        }

        Map<DataFormat, Map<String, DataFormatStoreHandler.FileEntry>> perStrategy = new HashMap<>();
        for (Map.Entry<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> entry : uploaded.entrySet()) {
            String file = entry.getKey();
            DataFormat owningFormat = null;
            StoreStrategy owning = null;
            for (Map.Entry<DataFormat, StoreStrategy> s : strategies.entrySet()) {
                if (s.getValue().owns(s.getKey().name(), file)) {
                    owningFormat = s.getKey();
                    owning = s.getValue();
                    break;
                }
            }
            if (owning == null || storeHandlers.containsKey(owningFormat) == false) {
                continue;
            }
            String blobKey = entry.getValue().getUploadedFilename();
            String basePath = remoteDirectory.getRemoteBasePath(owningFormat.name());
            String remotePath = owning.remotePath(owningFormat.name(), basePath, file, blobKey);
            // Use absolute path as key — matches what DataFusion uses for file:// lookups
            String absoluteKey = shardPath.getDataPath().resolve(file).toString();
            long size = entry.getValue().getLength();
            perStrategy.computeIfAbsent(owningFormat, k -> new HashMap<>())
                .put(absoluteKey, new DataFormatStoreHandler.FileEntry(remotePath, DataFormatStoreHandler.REMOTE, size));
        }

        for (Map.Entry<DataFormat, Map<String, DataFormatStoreHandler.FileEntry>> entry : perStrategy.entrySet()) {
            storeHandlers.get(entry.getKey()).seed(entry.getValue());
            logger.debug("Seeded {} files into store handler for format [{}]", entry.getValue().size(), entry.getKey().name());
        }
    }
}
