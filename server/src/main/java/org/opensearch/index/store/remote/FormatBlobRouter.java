/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Routes blob operations to format-specific {@link BlobContainer}s.
 *
 * <p>Encapsulates the mapping from data format names (e.g., "lucene", "parquet") to
 * their corresponding blob containers in the remote store. Formats that are considered
 * "base path" formats (lucene, metadata) share the root blob container. All other
 * formats get a sub-path container (e.g., {@code basePath/parquet/}).
 *
 * <p>Blob containers are created lazily on first access via {@link #containerFor(String)},
 * so new formats can be added without restarting the directory.
 *
 * <p>This class is thread-safe. Concurrent calls to {@link #containerFor(String)} for
 * the same format will produce the same container instance (via {@code computeIfAbsent}).
 *
 * @opensearch.internal
 */
@InternalApi
public class FormatBlobRouter {

    /** Formats that route to the base blob container (same path as single-format RemoteDirectory). */
    private static final Set<String> BASE_PATH_FORMATS = Set.of("lucene", "LUCENE", "metadata");

    private static final String DEFAULT_FORMAT = "lucene";

    private final BlobStore blobStore;
    private final BlobPath basePath;
    private final BlobContainer baseContainer;
    private final ConcurrentHashMap<String, BlobContainer> formatContainers;

    /**
     * Reverse lookup cache: maps remote blob keys (e.g., "_0.pqt__UUID") to their data format
     * (e.g., "parquet"). Used by download/delete operations where the caller only has a plain
     * blob key and needs to resolve which format container to route to.
     *
     * <p>Volatile reference to immutable map for atomic swap in {@link #replaceBlobFormatCache(Map)}.
     */
    private volatile Map<String, String> blobFormatCache = Map.of();

    /**
     * Creates a router with the given blob store and base path.
     * The base container is created immediately; format-specific containers are lazy.
     *
     * @param blobStore the blob store for creating containers
     * @param basePath the base blob path for this shard's remote segment store
     */
    public FormatBlobRouter(BlobStore blobStore, BlobPath basePath) {
        this.blobStore = blobStore;
        this.basePath = basePath;
        this.baseContainer = blobStore.blobContainer(basePath);
        this.formatContainers = new ConcurrentHashMap<>();
        // Pre-register the default format so containerFor("lucene") doesn't create a sub-path
        this.formatContainers.put(DEFAULT_FORMAT, baseContainer);
    }

    /**
     * Returns the blob container for the given format.
     *
     * <p>Base-path formats ("lucene", "metadata") return the root container.
     * All other formats return a sub-path container at {@code basePath/formatName/},
     * created lazily on first access.
     *
     * @param format the data format name (e.g., "lucene", "parquet"). Null defaults to "lucene".
     * @return the blob container for the format
     */
    public BlobContainer containerFor(String format) {
        if (format == null || format.isEmpty() || BASE_PATH_FORMATS.contains(format)) {
            return baseContainer;
        }
        return formatContainers.computeIfAbsent(format, this::createFormatContainer);
    }

    /**
     * Returns the base blob container (used for lucene/metadata files).
     * Equivalent to {@code containerFor("lucene")}.
     *
     * @return the base blob container
     */
    public BlobContainer baseContainer() {
        return baseContainer;
    }

    /**
     * Returns the base blob path.
     *
     * @return the base blob path
     */
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Lists all blobs across all known format containers.
     *
     * <p>Aggregates blobs from the base container and all format-specific containers
     * that have been accessed (lazily created). The returned map is unmodifiable.
     *
     * @return map of blob name to metadata across all format containers
     * @throws IOException if listing fails for any container
     */
    public Map<String, BlobMetadata> listAllBlobs() throws IOException {
        Map<String, BlobMetadata> all = new LinkedHashMap<>(baseContainer.listBlobs());
        for (Map.Entry<String, BlobContainer> entry : formatContainers.entrySet()) {
            // Skip the default format — already listed via baseContainer
            if (DEFAULT_FORMAT.equals(entry.getKey())) {
                continue;
            }
            all.putAll(entry.getValue().listBlobs());
        }
        return Collections.unmodifiableMap(all);
    }

    /**
     * Lists blobs in a specific format's container.
     *
     * @param format the data format name
     * @return map of blob name to metadata for the format
     * @throws IOException if listing fails
     */
    public Map<String, BlobMetadata> listBlobs(String format) throws IOException {
        return containerFor(format).listBlobs();
    }

    /**
     * Returns the set of all format names that have been accessed (have containers).
     * Always includes "lucene".
     *
     * @return unmodifiable set of registered format names
     */
    public Set<String> registeredFormats() {
        Set<String> formats = new LinkedHashSet<>();
        formats.add(DEFAULT_FORMAT);
        for (String format : formatContainers.keySet()) {
            if (DEFAULT_FORMAT.equals(format) == false) {
                formats.add(format);
            }
        }
        return Collections.unmodifiableSet(formats);
    }

    /**
     * Pre-registers a format so its container is created eagerly.
     * Useful during initialization when the set of formats is known from index settings.
     *
     * @param format the format name to pre-register
     */
    public void registerFormat(String format) {
        if (format != null && BASE_PATH_FORMATS.contains(format) == false) {
            formatContainers.computeIfAbsent(format, this::createFormatContainer);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Blob Format Cache — reverse lookup from blob key to format
    // ═══════════════════════════════════════════════════════════════

    /**
     * Registers a blob key → format mapping in the reverse lookup cache.
     * Called after a successful upload so that subsequent download/delete operations
     * can resolve the correct format container for a plain blob key.
     *
     * @param blobKey the remote blob key (e.g., "_0.pqt__UUID")
     * @param format the data format name (e.g., "parquet")
     */
    public void registerBlobFormat(String blobKey, String format) {
        if (blobKey != null && format != null) {
            var updated = new HashMap<>(blobFormatCache);
            updated.put(blobKey, format);
            blobFormatCache = Map.copyOf(updated);
        }
    }

    /**
     * Removes a blob key from the reverse lookup cache.
     * Called after a file is deleted from the remote store.
     *
     * @param blobKey the remote blob key to unregister
     */
    public void unregisterBlobFormat(String blobKey) {
        if (blobKey != null) {
            var updated = new HashMap<>(blobFormatCache);
            updated.remove(blobKey);
            blobFormatCache = Map.copyOf(updated);
        }
    }

    /**
     * Atomically replaces the entire blob format cache.
     * Called during initialization or full metadata refresh when the complete
     * mapping is rebuilt from remote segment metadata.
     *
     * @param blobKeyToFormat the new complete mapping of blob key to format
     */
    public void replaceBlobFormatCache(Map<String, String> blobKeyToFormat) {
        blobFormatCache = Map.copyOf(blobKeyToFormat);
    }

    /**
     * Resolves the data format for a plain blob key using the reverse lookup cache.
     * Returns the default format ("lucene") if the key is not found.
     *
     * @param blobKey the blob key to resolve
     * @return the data format name, defaults to "lucene"
     */
    public String resolveFormat(String blobKey) {
        String cached = blobFormatCache.get(blobKey);
        return cached != null ? cached : DEFAULT_FORMAT;
    }

    /**
     * Clears the blob format cache. Called during close/cleanup.
     */
    public void clearBlobFormatCache() {
        blobFormatCache = Map.of();
    }

    private BlobContainer createFormatContainer(String format) {
        BlobPath formatPath = Objects.requireNonNull(basePath.parent()).add(format.toLowerCase(Locale.ROOT));
        return blobStore.blobContainer(formatPath);
    }
}
