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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.NativeFileRegistry;
import org.opensearch.index.engine.dataformat.NativeFileRegistryFactory;
import org.opensearch.index.engine.dataformat.StoreStrategy;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
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
 * native file registries.
 *
 * <p>Owns the plumbing shared by every data format participating in the tiered
 * store so that format plugins stay purely declarative:
 * <ul>
 *   <li>resolves the owning {@link StoreStrategy} for a file</li>
 *   <li>constructs per-strategy {@link NativeFileRegistry} instances
 *       exception-safely (no leaked native resources if one factory throws)</li>
 *   <li>seeds registries from the remote segment metadata at open time</li>
 *   <li>forwards {@code onUploaded} / {@code onRemoved} events to the owning
 *       strategy's registry, if any</li>
 *   <li>closes registries in the right order when the shard shuts down</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class StoreStrategyRegistry implements Closeable {

    private static final Logger logger = LogManager.getLogger(StoreStrategyRegistry.class);

    /** Sentinel for "no strategies registered on this shard". Safe to close. */
    public static final StoreStrategyRegistry EMPTY = new StoreStrategyRegistry(
        Collections.emptyMap(),
        Collections.emptyMap()
    );

    /** Strategies keyed by format name. */
    private final Map<String, StoreStrategy> strategies;
    /** Native registries keyed by format name. Absent for strategies without one. */
    private final Map<String, NativeFileRegistry> nativeRegistries;

    /**
     * A strategy paired with the format name it is registered under. Used
     * internally for routing decisions so callers never need to re-derive the
     * name from the strategy.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public record Match(String name, StoreStrategy strategy) {}

    private StoreStrategyRegistry(Map<String, StoreStrategy> strategies, Map<String, NativeFileRegistry> nativeRegistries) {
        this.strategies = Map.copyOf(strategies);
        this.nativeRegistries = Map.copyOf(nativeRegistries);
    }

    /**
     * Builds a registry for a shard, constructing per-strategy native file
     * registries and seeding them from the remote metadata.
     *
     * <p>If any native-registry factory throws, all registries created so far
     * are closed and the exception is rethrown — no partial state escapes.
     *
     * @param shardId         the shard id
     * @param isWarm          true on warm nodes
     * @param nativeStore     the repository's native store, or
     *                        {@link NativeStoreRepository#EMPTY}
     * @param strategies      the strategies that apply to this shard, keyed by format name
     * @param remoteDirectory the remote segment store directory used to seed initial state
     * @return a fully-initialised registry
     */
    public static StoreStrategyRegistry open(
        ShardId shardId,
        boolean isWarm,
        NativeStoreRepository nativeStore,
        Map<String, StoreStrategy> strategies,
        RemoteSegmentStoreDirectory remoteDirectory
    ) {
        if (strategies == null || strategies.isEmpty()) {
            return EMPTY;
        }

        Map<String, NativeFileRegistry> nativeRegistries = new HashMap<>();
        List<NativeFileRegistry> created = new ArrayList<>();
        boolean success = false;
        try {
            for (Map.Entry<String, StoreStrategy> entry : strategies.entrySet()) {
                String name = entry.getKey();
                StoreStrategy strategy = entry.getValue();
                NativeFileRegistryFactory factory = strategy.nativeFileRegistry().orElse(null);
                if (factory == null) {
                    continue;
                }
                NativeFileRegistry registry = factory.create(shardId, isWarm, nativeStore);
                if (registry != null) {
                    nativeRegistries.put(name, registry);
                    created.add(registry);
                }
            }

            if (nativeRegistries.isEmpty() == false) {
                seedFromRemoteMetadata(strategies, nativeRegistries, remoteDirectory);
            }
            success = true;
            return new StoreStrategyRegistry(strategies, nativeRegistries);
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(created);
            }
        }
    }

    /**
     * Returns the strategy that owns {@code file}, or {@code null} if no
     * registered strategy claims it. The returned {@link Match} carries both
     * the registered name and the strategy object.
     */
    public Match matchFor(String file) {
        if (file == null) {
            return null;
        }
        for (Map.Entry<String, StoreStrategy> entry : strategies.entrySet()) {
            if (entry.getValue().owns(entry.getKey(), file)) {
                return new Match(entry.getKey(), entry.getValue());
            }
        }
        return null;
    }

    /** True if any strategy on this shard has a native file registry. */
    public boolean hasNativeRegistries() {
        return nativeRegistries.isEmpty() == false;
    }

    /**
     * Forwards a sync-to-remote event. Resolves the owning strategy, constructs
     * the remote path via {@link StoreStrategy#remotePath}, and forwards to the
     * native file registry for that strategy if one exists.
     *
     * @param file            the file identifier that was uploaded
     * @param basePath        the repository base path
     * @param uploadedBlobKey the blob key assigned by the upload path
     * @return true if the event was dispatched to a native registry; false if
     *         no strategy owns the file or the owning strategy has no native
     *         registry
     */
    public boolean onUploaded(String file, String basePath, String uploadedBlobKey) {
        Match match = matchFor(file);
        if (match == null) {
            return false;
        }
        NativeFileRegistry registry = nativeRegistries.get(match.name());
        if (registry == null) {
            return false;
        }
        registry.onUploaded(file, match.strategy().remotePath(match.name(), basePath, file, uploadedBlobKey));
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
        NativeFileRegistry registry = nativeRegistries.get(match.name());
        if (registry == null) {
            return false;
        }
        registry.onRemoved(file);
        return true;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(nativeRegistries.values());
    }

    private static void seedFromRemoteMetadata(
        Map<String, StoreStrategy> strategies,
        Map<String, NativeFileRegistry> nativeRegistries,
        RemoteSegmentStoreDirectory remoteDirectory
    ) {
        if (remoteDirectory == null) {
            return;
        }
        String basePath = remoteDirectory.getRemoteBasePath();
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> uploaded = remoteDirectory.getSegmentsUploadedToRemoteStore();
        if (uploaded == null || uploaded.isEmpty()) {
            return;
        }

        Map<String, Map<String, String>> perStrategy = new HashMap<>();
        for (Map.Entry<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> entry : uploaded.entrySet()) {
            String file = entry.getKey();
            String owningName = null;
            StoreStrategy owning = null;
            for (Map.Entry<String, StoreStrategy> s : strategies.entrySet()) {
                if (s.getValue().owns(s.getKey(), file)) {
                    owningName = s.getKey();
                    owning = s.getValue();
                    break;
                }
            }
            if (owning == null || nativeRegistries.containsKey(owningName) == false) {
                continue;
            }
            String blobKey = entry.getValue().getUploadedFilename();
            perStrategy
                .computeIfAbsent(owningName, k -> new HashMap<>())
                .put(file, owning.remotePath(owningName, basePath, file, blobKey));
        }

        for (Map.Entry<String, Map<String, String>> entry : perStrategy.entrySet()) {
            nativeRegistries.get(entry.getKey()).seed(entry.getValue());
            logger.debug(
                "Seeded {} files into native registry for format [{}]",
                entry.getValue().size(),
                entry.getKey()
            );
        }
    }
}
