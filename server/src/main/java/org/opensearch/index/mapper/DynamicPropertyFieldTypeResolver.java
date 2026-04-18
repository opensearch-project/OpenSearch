/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves {@link MappedFieldType} for field names that match {@link RootObjectMapper#dynamicProperties()}
 * without adding those fields to the serialized mapping. Query-time resolution mirrors ingest-time
 * {@link DynamicProperty} handling: no cluster state mapping update occurs when a name matches.
 *
 *
 * <p><b>Cache invalidation:</b> Each {@link DocumentMapper} constructor runs
 * {@link MappingLookup#fromMapping} and constructs a new resolver with an empty cache.
 * {@link DocumentMapper#merge} always returns a new {@link DocumentMapper} for the merged
 * {@link Mapping}, so {@link RootObjectMapper} / {@code dynamic_properties} updates never mutate
 * this instance in place; previous mappers (and their caches) are simply replaced on
 * {@link MapperService}. No explicit cache clear is required.
 *
 * <p><b>Concurrency:</b> {@link #resolve} is {@code synchronized} on this instance. The whole method
 * body (reads of {@code root} and {@code documentMapperParser}, {@link LinkedHashMap} get/put, and
 * LRU eviction via {@code removeEldestEntry}) runs under that single monitor, so there is no
 * unsynchronized access to non-thread-safe state. {@link LinkedHashMap} is never mutated outside
 * {@code resolve}. Other threads may hold different {@link DynamicPropertyFieldTypeResolver}
 * instances (per {@link DocumentMapper} snapshot); they do not share {@code cache}.
 * Serialization on {@code resolve} may contend under heavy query load; a future optimization could
 * use a concurrent map with bounded eviction if profiling warrants it. Regression:
 * {@code DynamicMappingTests#testDynamicPropertyFieldTypeLookupConcurrent}.
 *
 * <p>{@code root} and {@code documentMapperParser} are {@code final} and set at construction;
 * they are not replaced on this instance after publish, matching the immutable {@link DocumentMapper}
 * snapshot model above.
 *
 * @opensearch.internal
 */
final class DynamicPropertyFieldTypeResolver {

    private static final int MAX_RESOLVER_CACHE_SIZE = 1024;

    private final RootObjectMapper root;
    private final DocumentMapperParser documentMapperParser;
    /**
     * Accessed only from {@link #resolve} (synchronized on this); not thread-safe by itself but
     * correctly confined to that lock.
     */
    private final Map<String, MappedFieldType> cache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, MappedFieldType> eldest) {
            return size() > MAX_RESOLVER_CACHE_SIZE;
        }
    };

    DynamicPropertyFieldTypeResolver(RootObjectMapper root, DocumentMapperParser documentMapperParser) {
        this.root = root;
        this.documentMapperParser = documentMapperParser;
    }

    /**
     * Returns a field type for the given full path, or null if no dynamic_property applies.
     * {@code synchronized} (this): all use of {@link #cache}, {@link #root}, and
     * {@link #documentMapperParser} in this call is confined to this lock.
     */
    synchronized MappedFieldType resolve(String fullFieldName) {
        if (root.dynamicProperties().length == 0) {
            return null;
        }
        MappedFieldType cached = cache.get(fullFieldName);
        if (cached != null) {
            return cached;
        }
        DynamicProperty dp = root.findDynamicProperty(fullFieldName);
        if (dp == null) {
            return null;
        }
        String leaf = leafName(fullFieldName);
        Map<String, Object> config = new HashMap<>(dp.mappingForName(leaf));
        Object typeNode = config.get("type");
        if (typeNode == null) {
            return null;
        }
        String type = typeNode.toString();
        Mapper.TypeParser.ParserContext parserContext = documentMapperParser.parserContext();
        Mapper.TypeParser typeParser = parserContext.typeParser(type);
        if (typeParser == null) {
            return null;
        }
        Mapper.Builder<?> builder = typeParser.parse(leaf, config, parserContext);
        ContentPath path = parentPath(fullFieldName);
        Mapper.BuilderContext builderContext = new Mapper.BuilderContext(
            documentMapperParser.mapperService.getIndexSettings().getSettings(),
            path
        );
        Mapper mapper = builder.build(builderContext);
        if ((mapper instanceof FieldMapper) == false) {
            return null;
        }
        MappedFieldType fieldType = ((FieldMapper) mapper).fieldType();
        cache.put(fullFieldName, fieldType);
        return fieldType;
    }

    /** Last path segment; used for {@link DynamicProperty#mappingForName} and type parser simple name. */
    private static String leafName(String fullFieldName) {
        int lastDot = fullFieldName.lastIndexOf('.');
        return lastDot < 0 ? fullFieldName : fullFieldName.substring(lastDot + 1);
    }

    /** Content path for all parent segments (empty at root). */
    private static ContentPath parentPath(String fullFieldName) {
        ContentPath path = new ContentPath();
        int lastDot = fullFieldName.lastIndexOf('.');
        if (lastDot <= 0) {
            return path;
        }
        String prefix = fullFieldName.substring(0, lastDot);
        for (String segment : prefix.split("\\.")) {
            path.add(segment);
        }
        return path;
    }
}
