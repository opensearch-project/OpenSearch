/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Caches precomputed stats from catalog snapshots to avoid expensive recomputation on every API call.
 * Stats are refreshed only when the catalog snapshot changes (on refresh), not on every stats API call.
 *
 * This optimization moves expensive stats computation from API-call time (frequent) to refresh time (infrequent),
 * significantly improving performance for frequently called stats APIs.
 */
public class CatalogSnapshotStatsCache implements ReferenceManager.RefreshListener {

    // Cached stats - volatile for thread safety between refresh and API threads
    private volatile DocsStats cachedDocsStats;
    private volatile SegmentsStats cachedSegmentsStats;
    private volatile List<Segment> cachedSegments;

    private final CatalogSnapshotManager snapshotManager;
    private final Store store;
    private final EngineConfig engineConfig;
    private final Supplier<Map<String, String>> lastCommitDataSupplier;
    private final Logger logger;

    public CatalogSnapshotStatsCache(
        CatalogSnapshotManager snapshotManager,
        Store store,
        EngineConfig engineConfig,
        Supplier<Map<String, String>> lastCommitDataSupplier,
        Logger logger
    ) {
        this.snapshotManager = snapshotManager;
        this.store = store;
        this.engineConfig = engineConfig;
        this.lastCommitDataSupplier = lastCommitDataSupplier;
        this.logger = logger;
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (didRefresh) {
            refreshCachedStats();
        }
    }

    @Override
    public void beforeRefresh() throws IOException {
        // No action needed before refresh
    }

    private void refreshCachedStats() {
        try (GatedCloseable<CatalogSnapshot> snapshotRef = snapshotManager.acquireSnapshot()) {
            CatalogSnapshot snapshot = snapshotRef.get();

            // Precompute catalog-specific stats
            DocsStats newDocsStats = computeDocsStats(snapshot);
            SegmentsStats newSegmentsStats = computeSegmentsStats(snapshot);

            // For segments, we need engine-specific data (indexSort, commit status)
            // This should be computed by the engine, not the cache
            // For now, we'll compute basic segments without engine-specific data
            boolean isCommitted = snapshot.resolveIsCommitted(lastCommitDataSupplier.get());
            List<Segment> newSegments = snapshot.buildEngineSegments(
                lastCommitDataSupplier.get(),
                engineConfig != null ? engineConfig.getIndexSort() : null
            );

            // Atomic replacement - all or nothing for consistency
            this.cachedDocsStats = newDocsStats;
            this.cachedSegmentsStats = newSegmentsStats;
            this.cachedSegments = newSegments;

            logger.debug("Refreshed cached catalog snapshot stats: {} docs, {} segments", newDocsStats.getCount(), newSegments.size());

        } catch (Exception e) {
            logger.warn("Failed to refresh cached catalog snapshot stats", e);
            // Keep existing cached values on error - better than failing stats APIs
        }
    }

    private DocsStats computeDocsStats(CatalogSnapshot snapshot) {
        // Compute docs stats from catalog snapshot
        long totalDocs = snapshot.getNumDocs();
        long deletedDocs = 0; // TODO: Add deleted docs support when available
        long totalSizeInBytes = 0; // TODO: Compute total size if needed

        return new DocsStats.Builder().count(totalDocs).deleted(deletedDocs).totalSizeInBytes(totalSizeInBytes).build();
    }

    private SegmentsStats computeSegmentsStats(CatalogSnapshot snapshot) {
        return buildSegmentsStats(
            0L, // nativeBytesUsed - engine-specific, passed from outside
            -1L, // maxUnsafeAutoIdTimestamp - engine-specific, passed from outside
            snapshot
        );
    }

    /**
     * Constructs a {@link SegmentsStats} with memory and timestamp passed from outside,
     * while calculating segment count and file sizes internally from the snapshot.
     * File sizes are grouped by extension (like internal engine) for compound files.
     *
     * @param nativeBytesUsed          native/off-heap memory used by the engine (passed from outside)
     * @param maxUnsafeAutoIdTimestamp max unsafe auto-id timestamp (passed from outside)
     * @param snapshot                 catalog snapshot to calculate internal stats from
     * @return populated {@link SegmentsStats}
     */
    public SegmentsStats buildSegmentsStats(long nativeBytesUsed, long maxUnsafeAutoIdTimestamp, CatalogSnapshot snapshot) {
        // Calculate internal values from snapshot
        int segmentCount = snapshot.getSegments().size();
        Map<String, Long> rawFileSizes = snapshot.collectFileSizes(store);

        SegmentsStats stats = new SegmentsStats();
        stats.add(segmentCount);
        stats.addIndexWriterMemoryInBytes(nativeBytesUsed);
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp);

        // Group file sizes by extension (like internal engine does)
        if (rawFileSizes != null && !rawFileSizes.isEmpty()) {
            Map<String, Long> groupedFileSizes = groupFilesByExtension(rawFileSizes);
            stats.addFileSizes(groupedFileSizes);
        }

        return stats;
    }

    /**
     * Groups file sizes by extension, extracting the underlying format extension for compound files.
     * This matches the behavior of the internal engine for consistent file size reporting.
     */
    private Map<String, Long> groupFilesByExtension(Map<String, Long> rawFileSizes) {
        Map<String, Long> grouped = new HashMap<>();

        for (Map.Entry<String, Long> entry : rawFileSizes.entrySet()) {
            String fileName = entry.getKey();
            Long fileSize = entry.getValue();

            // Extract extension from filename
            String extension = getFileExtension(fileName);

            // Merge sizes for the same extension
            grouped.merge(extension, fileSize, Long::sum);
        }

        return grouped;
    }

    /**
     * Extracts the file extension from a filename.
     * For compound files, this should extract the underlying format extension.
     */
    private String getFileExtension(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return "unknown";
        }

        int lastDot = fileName.lastIndexOf('.');
        if (lastDot == -1 || lastDot == fileName.length() - 1) {
            return "unknown";
        }

        return fileName.substring(lastDot + 1);
    }

    // Fast API methods - just return cached values

    /**
     * Returns precomputed docs stats. Called frequently by stats APIs.
     * @return cached DocsStats or empty stats if not yet computed
     */
    public DocsStats getDocsStats() {
        DocsStats cached = cachedDocsStats;
        return cached != null ? cached : new DocsStats();
    }

    /**
     * Returns precomputed segments stats. Called frequently by stats APIs.
     * @return cached SegmentsStats or empty stats if not yet computed
     */
    public SegmentsStats getSegmentsStats() {
        SegmentsStats cached = cachedSegmentsStats;
        return cached != null ? cached : new SegmentsStats();
    }

    /**
     * Returns precomputed segments list. Called frequently by stats APIs.
     * @return cached segments list or empty list if not yet computed
     */
    public List<Segment> getSegments() {
        List<Segment> cached = cachedSegments;
        return cached != null ? cached : Collections.emptyList();
    }

    /**
     * Forces a refresh of cached stats. Useful for initialization or testing.
     */
    public void forceRefresh() {
        refreshCachedStats();
    }
}
