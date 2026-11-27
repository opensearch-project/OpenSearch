/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Node-level cache of the field mappings of indices, so an {@link IndexAwareSearchRequestProcessor} across
 * many requests over time &mdash; not just within one request &mdash; never re-parses an unchanged mapping.
 * <p>
 * {@code MappingMetadata#sourceAsMap()} decompresses and re-parses the mapping on every call, with no
 * caching of its own. Without this, an alias over many indices with large mappings pays that cost on every
 * single search, forever, even when nothing about those mappings ever changes.
 * <p>
 * Only the {@code properties} section of a mapping is kept: the field definitions are what a processor
 * reads. The rest of the mapping ({@code dynamic}, {@code _meta}, {@code _routing}, {@code _source},
 * {@code dynamic_templates}, {@code runtime}) is not held in memory for it.
 * <p>
 * Holds at most one entry per index UUID: the field mappings of the newest mapping version any request has
 * asked for. A mapping update replaces that index's entry instead of adding a second one, and
 * {@link #remove(Index)} (called from a cluster state listener when an index is deleted) drops it.
 * <p>
 * Like the request cache's on-heap store, entries live in a {@link Cache} bounded by estimated heap bytes
 * and evicted least recently used first. The estimate is taken while the parsed mapping is copied into its
 * unmodifiable form, since {@link RamUsageEstimator#sizeOfObject(Object)} only looks one level into nested
 * maps, and a mapping is mostly nesting.
 * <p>
 * Cached maps are deeply unmodifiable. One instance is shared by every request and thread on the node, so
 * a processor that mutated any level of it would corrupt every other request's view of that mapping.
 *
 * @opensearch.internal
 */
public final class IndexMappingSourceCache {

    /**
     * The estimated heap the parsed field mappings cached for {@link IndexAwareSearchRequestProcessor}s may
     * take on a node, as a size or a percentage of the heap, like {@code indices.requests.cache.size}. Beyond
     * it, the least recently used index's entry is evicted, and a search on that index parses its mapping
     * again. {@code 0} caches nothing. Read once at node start.
     */
    public static final Setting<ByteSizeValue> SIZE_SETTING = Setting.memorySizeSetting(
        "search.index_aware_search_request_processors.mapping_cache.size",
        "1%",
        Property.NodeScope
    );

    // Estimated heap cost of the structures deepUnmodifiable builds, beyond the keys and values they hold.
    private static final long MAP_BYTES = RamUsageEstimator.shallowSizeOfInstance(LinkedHashMap.class) + RamUsageEstimator
        .shallowSizeOfInstance(Collections.unmodifiableMap(Map.of()).getClass());
    // A LinkedHashMap entry: hash, key, value, next, before, after.
    private static final long MAP_ENTRY_BYTES = RamUsageEstimator.alignObjectSize(
        RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + Integer.BYTES + 5L * RamUsageEstimator.NUM_BYTES_OBJECT_REF
    );
    private static final long LIST_BYTES = RamUsageEstimator.shallowSizeOfInstance(ArrayList.class) + RamUsageEstimator
        .shallowSizeOfInstance(Collections.unmodifiableList(new ArrayList<>()).getClass());
    private static final long ENTRY_BYTES = RamUsageEstimator.shallowSizeOfInstance(Entry.class);

    private final Cache<String, Entry> byIndexUuid;

    /** Creates a cache of the default {@link #SIZE_SETTING} size. */
    public IndexMappingSourceCache() {
        this(Settings.EMPTY);
    }

    /**
     * Creates a cache of the size {@link #SIZE_SETTING} gives in {@code settings}.
     *
     * @param settings the node settings
     */
    public IndexMappingSourceCache(Settings settings) {
        this(SIZE_SETTING.get(settings).getBytes());
    }

    /**
     * Creates a cache that keeps field mappings up to an estimated {@code maxWeightInBytes} of heap.
     *
     * @param maxWeightInBytes the estimated heap the cached field mappings may take; 0 caches nothing
     */
    IndexMappingSourceCache(long maxWeightInBytes) {
        this.byIndexUuid = CacheBuilder.<String, Entry>builder()
            .setMaximumWeight(maxWeightInBytes)
            .weigher((indexUuid, entry) -> RamUsageEstimator.sizeOf(indexUuid) + entry.ramBytesUsed)
            .build();
    }

    /**
     * Returns the field mappings of the index {@code metadata} describes: the {@code properties} section of
     * its mapping, parsed into a map from field name to that field's mapping. A second call for the same
     * index at the same mapping version returns the same instance without parsing again, whichever request
     * or processor it comes from, as long as the entry has not been evicted in between.
     *
     * @param metadata the index whose field mappings to return
     * @return the field mappings, unmodifiable at every level, or an empty map if the index has no mapping
     *         or its mapping has no fields
     */
    public Map<String, Object> getFieldMappings(IndexMetadata metadata) {
        if (metadata.mapping() == null) {
            return Collections.emptyMap();
        }
        final String indexUuid = metadata.getIndexUUID();
        if (IndexMetadata.INDEX_UUID_NA_VALUE.equals(indexUuid)) {
            // Every index in a cluster state has a UUID of its own. Metadata built without one (in a test,
            // typically) shares this placeholder with all other such metadata, so it cannot be a cache key.
            return parse(metadata).fieldMappings;
        }
        final long mappingVersion = metadata.getMappingVersion();
        final Entry cached;
        try {
            // Parses at most once per index under contention, outside the cache's locks.
            cached = byIndexUuid.computeIfAbsent(indexUuid, uuid -> parse(metadata));
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to parse the mapping of " + metadata.getIndex(), e.getCause());
        }
        if (cached.mappingVersion == mappingVersion) {
            return cached.fieldMappings;
        }
        final Entry parsed = parse(metadata);
        if (cached.mappingVersion < mappingVersion) {
            // The mapping was updated since the entry was cached: replace it. Two requests on two newer
            // versions may race here and leave the older of them cached; the next request on the newest
            // version replaces it again. Either way, each caller gets the mapping of its own version.
            byIndexUuid.put(indexUuid, parsed);
        }
        // Otherwise the request still holds an older cluster state than one this cache has already seen.
        // Rare and transient: it gets its own version, parsed for it alone, and the newer entry stays.
        return parsed.fieldMappings;
    }

    /**
     * Drops the cached field mappings of {@code index}, if there are any. Called when the index is deleted.
     *
     * @param index the deleted index
     */
    public void remove(Index index) {
        byIndexUuid.invalidate(index.getUUID());
    }

    /** Number of entries currently cached, for tests. */
    int size() {
        return byIndexUuid.count();
    }

    /** Estimated heap taken by the cached entries, for tests. */
    long weight() {
        return byIndexUuid.weight();
    }

    /** Whether {@code index} has an entry, for tests. Counts as a use of that entry. */
    boolean contains(Index index) {
        return byIndexUuid.get(index.getUUID()) != null;
    }

    private static Entry parse(IndexMetadata metadata) {
        final Object properties = metadata.mapping().sourceAsMap().get("properties");
        final long[] bytes = { ENTRY_BYTES };
        final Map<String, Object> fieldMappings = properties instanceof Map<?, ?> fields
            ? deepUnmodifiableMap(fields, bytes)
            : Collections.emptyMap();
        return new Entry(metadata.getMappingVersion(), fieldMappings, bytes[0]);
    }

    private static Map<String, Object> deepUnmodifiableMap(Map<?, ?> map, long[] bytes) {
        bytes[0] += MAP_BYTES + RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) tableSize(map.size()) * RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
        final Map<String, Object> copy = new LinkedHashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            final String key = String.valueOf(entry.getKey());
            bytes[0] += MAP_ENTRY_BYTES + RamUsageEstimator.sizeOf(key);
            copy.put(key, deepUnmodifiable(entry.getValue(), bytes));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Object deepUnmodifiable(Object value, long[] bytes) {
        if (value instanceof Map<?, ?> map) {
            return deepUnmodifiableMap(map, bytes);
        }
        if (value instanceof List<?> list) {
            bytes[0] += LIST_BYTES + RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) list.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF
            );
            final List<Object> copy = new ArrayList<>(list.size());
            for (Object element : list) {
                copy.add(deepUnmodifiable(element, bytes));
            }
            return Collections.unmodifiableList(copy);
        }
        // A string, number or boolean: sizeOfObject measures these exactly.
        bytes[0] += RamUsageEstimator.sizeOfObject(value);
        return value;
    }

    /** The hash table length a {@code LinkedHashMap} sized for {@code size} entries allocates. */
    private static int tableSize(int size) {
        final int needed = (int) Math.ceil(size / 0.75);
        return needed <= 1 ? 1 : Integer.highestOneBit(needed - 1) << 1;
    }

    private record Entry(long mappingVersion, Map<String, Object> fieldMappings, long ramBytesUsed) {
    }
}
