/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Point-in-time snapshot of {@link BlockCache} counters.
 *
 * <p>Emitted for node-stats reporting and merging into
 * {@link org.opensearch.index.store.remote.filecache.AggregateFileCacheStats}.
 * Every {@code BlockCache} implementation is expected to surface all counters;
 * implementations that do not track a particular metric should return zero for
 * that field rather than throwing.
 *
 * <h3>Field semantics</h3>
 * <ul>
 *   <li>{@code hits}            — cumulative count of lookups served from the cache.</li>
 *   <li>{@code misses}          — cumulative count of lookups that did not find an entry.</li>
 *   <li>{@code hitBytes}        — cumulative bytes served from cache hits
 *       (bandwidth actually saved by the cache).</li>
 *   <li>{@code missBytes}       — cumulative bytes fetched on cache misses
 *       (bytes that had to be read from the backing store).</li>
 *   <li>{@code evictions}       — cumulative count of LRU / capacity-driven eviction events.</li>
 *   <li>{@code evictionBytes}   — cumulative bytes displaced by evictions.
 *       Used by {@code UnifiedCacheService.mergeStats()} to populate
 *       {@code evictions_in_bytes} in the REST output.</li>
 *   <li>{@code removed}         — cumulative count of explicit invalidations
 *       (e.g. delete-index, segment rename, merge completion).
 *       Mirrors the {@code removed} field in {@code FileCacheStats}.</li>
 *   <li>{@code removedBytes}    — cumulative bytes explicitly removed.
 *       Used by {@code UnifiedCacheService.mergeStats()} to populate
 *       {@code removed_in_bytes} in the REST output.</li>
 *   <li>{@code memoryBytesUsed} — current bytes occupied in the in-memory tier
 *       (zero for disk-only implementations such as Foyer).</li>
 *   <li>{@code diskBytesUsed}   — current bytes occupied in the on-disk tier
 *       (zero for memory-only implementations).</li>
 *   <li>{@code totalBytes}      — configured total capacity (memory + disk).
 *       Used by {@code UnifiedCacheService.mergeStats()} to populate
 *       {@code total_in_bytes} in the REST output.</li>
 * </ul>
 *
 * <p>Values are a snapshot at construction time and are not guaranteed to be
 * internally consistent across concurrent cache activity.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheStats(
    long hits,
    long misses,
    long hitBytes,
    long missBytes,
    long evictions,
    long evictionBytes,
    long removed,
    long removedBytes,
    long memoryBytesUsed,
    long diskBytesUsed,
    long totalBytes
) {
}
