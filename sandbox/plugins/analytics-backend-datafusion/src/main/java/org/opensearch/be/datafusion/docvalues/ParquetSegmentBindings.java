/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.opensearch.core.index.shard.ShardId;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Node-level, per-shard registry mapping a segment's writer generation to the Parquet file that backs
 * its doc values and the native store to read that file through.
 *
 * <p><b>Why this exists.</b> The Parquet doc-values codec used to read its backing file from two
 * {@code SegmentInfo} attributes ({@code parquet.docvalues.file}/{@code parquet.docvalues.store_ptr})
 * that the composite engine stamped onto each Lucene leaf at searcher-acquire time. That stamping has
 * been removed from the server: the plugin now owns the mapping end to end. {@code DatafusionReaderManager}
 * populates this registry from the {@code CatalogSnapshot} on every refresh (the single place that sees
 * both the catalog's parquet {@code WriterFileSet}s and the shard's native store handle), and
 * {@code ParquetSegmentLayout#resolve} reads it back at reader-wrap time, correlating on the segment's
 * {@code writer_generation} attribute (which equals the catalog {@code Segment}'s generation).
 *
 * <p><b>Retention rule.</b> Bindings are tracked per {@code (shard, snapshotId)}. A generation must stay
 * resolvable while <em>any</em> live snapshot of that shard still references it, because different live
 * searchers may pin different snapshots that share an unchanged (e.g. un-merged) segment. Accordingly
 * {@link #resolve} searches a shard's live snapshots newest-first and returns the first binding found for
 * the generation; a snapshot's bindings are pruned only when that snapshot is released
 * ({@link #release}, from the reader manager's {@code onDeleted}) or the shard's manager closes
 * ({@link #removeShard}, from {@code close}).
 *
 * <p><b>Threading.</b> {@link #register}/{@link #release} run on refresh/delete threads; {@link #resolve}
 * runs on search threads at wrap time. Per-shard mutations are made atomic on the shard bin via
 * {@link ConcurrentHashMap#compute}/{@link ConcurrentHashMap#computeIfPresent}, snapshot maps use a
 * {@link ConcurrentSkipListMap} (sorted by snapshot id for newest-first traversal), and each snapshot's
 * generation map is stored as an immutable copy, so {@link #resolve} is lock-free and always sees a
 * consistent binding set.
 */
public final class ParquetSegmentBindings {

    /** Where one segment generation's Parquet doc values live: the file and the store to read it through. */
    public record Binding(Path parquetFile, long storePointer) {
    }

    // ShardId -> (snapshotId -> immutable generation->Binding map). The inner map is sorted so
    // descendingMap() yields the shard's snapshots newest-first for the retention search.
    private final ConcurrentHashMap<ShardId, ConcurrentSkipListMap<Long, Map<Long, Binding>>> byShard = new ConcurrentHashMap<>();

    /**
     * Records {@code generationBindings} for {@code (shardId, snapshotId)}, replacing any prior bindings
     * for that same snapshot. An empty map is still recorded so the snapshot is tracked and later
     * {@link #release}d cleanly.
     */
    public void register(ShardId shardId, long snapshotId, Map<Long, Binding> generationBindings) {
        Map<Long, Binding> immutable = Map.copyOf(generationBindings);
        // compute keeps the put atomic on the shard bin, so it cannot race a concurrent release that
        // prunes the shard to empty.
        byShard.compute(shardId, (k, bySnapshot) -> {
            if (bySnapshot == null) {
                bySnapshot = new ConcurrentSkipListMap<>();
            }
            bySnapshot.put(snapshotId, immutable);
            return bySnapshot;
        });
    }

    /**
     * Drops the bindings recorded for {@code (shardId, snapshotId)}, pruning the shard entry entirely
     * when its last live snapshot is released. Generations shared with a still-live snapshot remain
     * resolvable through that snapshot.
     */
    public void release(ShardId shardId, long snapshotId) {
        byShard.computeIfPresent(shardId, (k, bySnapshot) -> {
            bySnapshot.remove(snapshotId);
            return bySnapshot.isEmpty() ? null : bySnapshot;
        });
    }

    /** Drops every binding for {@code shardId}; used when the shard's reader manager closes. */
    public void removeShard(ShardId shardId) {
        byShard.remove(shardId);
    }

    /**
     * The binding for {@code generation} on {@code shardId}, searching the shard's live snapshots
     * newest-first, or {@code null} if no live snapshot references that generation.
     */
    public Binding resolve(ShardId shardId, long generation) {
        ConcurrentSkipListMap<Long, Map<Long, Binding>> bySnapshot = byShard.get(shardId);
        if (bySnapshot == null) {
            return null;
        }
        // Newest-first: the highest snapshot id is the most recent snapshot of this shard.
        for (Map<Long, Binding> generationBindings : bySnapshot.descendingMap().values()) {
            Binding binding = generationBindings.get(generation);
            if (binding != null) {
                return binding;
            }
        }
        return null;
    }
}
