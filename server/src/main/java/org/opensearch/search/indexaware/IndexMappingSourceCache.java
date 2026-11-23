/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.Index;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level cache of parsed index mappings, so an {@link IndexAwareSearchRequestProcessor} across many
 * requests over time &mdash; not just within one request &mdash; never re-parses an unchanged mapping.
 * <p>
 * {@code MappingMetadata#sourceAsMap()} decompresses and re-parses the mapping on every call, with no
 * caching of its own. Without this, an alias over many indices with large mappings pays that cost on every
 * single search, forever, even when nothing about those mappings ever changes.
 * <p>
 * Holds at most one parsed mapping per index UUID: the newest mapping version any request has asked for. A
 * mapping update replaces that index's entry instead of adding a second one, and {@link #remove(Index)}
 * (called from a cluster state listener when an index is deleted) drops it. That bounds this cache's memory
 * by the mappings of the indices that currently exist and have actually been looked up through the hook,
 * the same order of memory a data node already spends on those indices' {@code MapperService}s, rather than
 * by an entry count that says nothing about how large each entry is.
 * <p>
 * A hit is a single lock-free {@link ConcurrentHashMap#get}: this sits on the search path, once per target
 * index per processor per request, so it deliberately isn't built on the general-purpose LRU
 * {@code Cache}, whose every read takes one cache-wide lock.
 * <p>
 * Cached maps are deeply unmodifiable. One instance is shared by every request and thread on the node, so
 * a processor that mutated any level of it would corrupt every other request's view of that mapping.
 *
 * @opensearch.internal
 */
public final class IndexMappingSourceCache {

    private final ConcurrentHashMap<String, Entry> byIndexUuid = new ConcurrentHashMap<>();

    /**
     * Returns the mapping of the index {@code metadata} describes, parsed into a {@code Map}. A second
     * call for the same index at the same mapping version returns the same instance without parsing again,
     * whichever request or processor it comes from.
     *
     * @param metadata the index whose mapping to return
     * @return the parsed mapping, unmodifiable at every level, or an empty map if the index has no mapping
     */
    public Map<String, Object> getMappingSourceAsMap(IndexMetadata metadata) {
        if (metadata.mapping() == null) {
            return Collections.emptyMap();
        }
        final String indexUuid = metadata.getIndexUUID();
        if (IndexMetadata.INDEX_UUID_NA_VALUE.equals(indexUuid)) {
            // Every index in a cluster state has a UUID of its own. Metadata built without one (in a test,
            // typically) shares this placeholder with all other such metadata, so it cannot be a cache key.
            return parse(metadata);
        }
        final long mappingVersion = metadata.getMappingVersion();
        Entry entry = byIndexUuid.get(indexUuid);
        if (entry == null || entry.mappingVersion < mappingVersion) {
            // compute() parses at most once per (index, version) under contention, and never replaces a
            // newer version with an older one.
            entry = byIndexUuid.compute(
                indexUuid,
                (uuid, current) -> current != null && current.mappingVersion >= mappingVersion
                    ? current
                    : new Entry(mappingVersion, parse(metadata))
            );
        }
        if (entry.mappingVersion == mappingVersion) {
            return entry.source;
        }
        // A request still holding an older cluster state than one this cache has already seen. Rare and
        // transient: parse for this caller alone rather than evict the newer entry everyone else wants.
        return parse(metadata);
    }

    /**
     * Drops the cached mapping of {@code index}, if there is one. Called when the index is deleted.
     *
     * @param index the deleted index
     */
    public void remove(Index index) {
        byIndexUuid.remove(index.getUUID());
    }

    /** Number of entries currently cached, for tests. */
    int size() {
        return byIndexUuid.size();
    }

    private static Map<String, Object> parse(IndexMetadata metadata) {
        return deepUnmodifiableMap(metadata.mapping().sourceAsMap());
    }

    private static Map<String, Object> deepUnmodifiableMap(Map<?, ?> map) {
        final Map<String, Object> copy = new LinkedHashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            copy.put(String.valueOf(entry.getKey()), deepUnmodifiable(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static Object deepUnmodifiable(Object value) {
        if (value instanceof Map<?, ?> map) {
            return deepUnmodifiableMap(map);
        }
        if (value instanceof List<?> list) {
            final List<Object> copy = new ArrayList<>(list.size());
            for (Object element : list) {
                copy.add(deepUnmodifiable(element));
            }
            return Collections.unmodifiableList(copy);
        }
        return value;
    }

    private record Entry(long mappingVersion, Map<String, Object> source) {
    }
}
