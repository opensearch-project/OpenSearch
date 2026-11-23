/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class IndexAwareContextTests extends OpenSearchTestCase {

    public void testGetTargetIndices() {
        IndexMetadata metadata = indexWithMapping("my-index", Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexAwareContext context = contextFor(Map.of("my-index", metadata), new IndexMappingSourceCache());

        assertEquals(Map.of("my-index", metadata), context.getTargetIndices());
    }

    public void testGetTargetIndicesIsUnmodifiable() {
        IndexMetadata metadata = indexWithMapping("my-index", Map.of("properties", Map.of()));
        IndexAwareContext context = contextFor(new HashMap<>(Map.of("my-index", metadata)), new IndexMappingSourceCache());

        assertThrows(UnsupportedOperationException.class, () -> context.getTargetIndices().put("other-index", metadata));
    }

    public void testGetMappingSourceAsMap() {
        Map<String, Object> mapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        IndexAwareContext context = contextFor(Map.of("my-index", indexWithMapping("my-index", mapping)), new IndexMappingSourceCache());

        assertEquals(mapping, context.getMappingSourceAsMap("my-index"));
    }

    // Two indices behind one request that map the same field differently: each index reports its own
    // mapping, so a processor can tell them apart.
    public void testGetMappingSourceAsMapIsPerIndex() {
        Map<String, Object> textMapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        Map<String, Object> keywordMapping = Map.of("properties", Map.of("title", Map.of("type", "keyword")));
        IndexAwareContext context = contextFor(
            Map.of(
                "text-index",
                indexWithMapping("text-index", textMapping),
                "keyword-index",
                indexWithMapping("keyword-index", keywordMapping)
            ),
            new IndexMappingSourceCache()
        );

        assertEquals(textMapping, context.getMappingSourceAsMap("text-index"));
        assertEquals(keywordMapping, context.getMappingSourceAsMap("keyword-index"));
    }

    // Two contexts stand in for two requests. Backed by the same node-level cache, the second must get
    // the map the first one parsed.
    public void testGetMappingSourceAsMapIsSharedAcrossContexts() {
        IndexMetadata metadata = indexWithMapping("my-index", Map.of("properties", Map.of()));
        IndexMappingSourceCache sharedCache = new IndexMappingSourceCache();
        IndexAwareContext firstRequestContext = contextFor(Map.of("my-index", metadata), sharedCache);
        IndexAwareContext secondRequestContext = contextFor(Map.of("my-index", metadata), sharedCache);

        Map<String, Object> first = firstRequestContext.getMappingSourceAsMap("my-index");
        Map<String, Object> second = secondRequestContext.getMappingSourceAsMap("my-index");
        assertSame(first, second);
    }

    public void testGetMappingSourceAsMapUnknownIndexIsEmpty() {
        IndexAwareContext context = contextFor(Collections.emptyMap(), new IndexMappingSourceCache());

        assertEquals(Collections.emptyMap(), context.getMappingSourceAsMap("not-a-target-index"));
    }

    public void testGetMappingSourceAsMapNoMappingIsEmpty() {
        IndexMetadata metadata = IndexMetadata.builder("my-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexAwareContext context = contextFor(Map.of("my-index", metadata), new IndexMappingSourceCache());

        assertNull(metadata.mapping());
        assertEquals(Collections.emptyMap(), context.getMappingSourceAsMap("my-index"));
    }

    // The field filter is applied per index: a field hidden in one index stays visible in another.
    public void testGetFieldFilterIsPerIndex() {
        IndexMetadata metadata = indexWithMapping("my-index", Map.of("properties", Map.of()));
        Function<String, Predicate<String>> hideSecretInRestrictedIndex = index -> "restricted-index".equals(index)
            ? field -> "secret".equals(field) == false
            : field -> true;
        IndexAwareContext context = new IndexAwareContext(
            Map.of("restricted-index", metadata, "open-index", metadata),
            new IndexMappingSourceCache(),
            hideSecretInRestrictedIndex,
            Version.CURRENT,
            TaskId.EMPTY_TASK_ID,
            true
        );

        assertFalse(context.getFieldFilter("restricted-index").test("secret"));
        assertTrue(context.getFieldFilter("restricted-index").test("title"));
        assertTrue(context.getFieldFilter("open-index").test("secret"));
    }

    // The mapping a processor reads is the whole mapping, whatever the field filter hides: hiding fields
    // from the caller is the processor's job, through getFieldFilter.
    public void testGetMappingSourceAsMapIsNotReducedByTheFieldFilter() {
        Map<String, Object> mapping = Map.of("properties", Map.of("secret", Map.of("type", "text")));
        IndexAwareContext context = new IndexAwareContext(
            Map.of("my-index", indexWithMapping("my-index", mapping)),
            new IndexMappingSourceCache(),
            index -> field -> false,
            Version.CURRENT,
            TaskId.EMPTY_TASK_ID,
            true
        );

        assertEquals(mapping, context.getMappingSourceAsMap("my-index"));
    }

    public void testAllowPartialSearchResultsWhenTheRequestAllowsThem() {
        assertTrue(contextAllowingPartialResults(true).allowPartialSearchResults());
    }

    public void testAllowPartialSearchResultsWhenTheRequestForbidsThem() {
        assertFalse(contextAllowingPartialResults(false).allowPartialSearchResults());
    }

    public void testGetMinNodeVersion() {
        IndexAwareContext context = new IndexAwareContext(
            Collections.emptyMap(),
            new IndexMappingSourceCache(),
            MapperPlugin.NOOP_FIELD_FILTER,
            Version.V_3_0_0,
            TaskId.EMPTY_TASK_ID,
            true
        );

        assertEquals(Version.V_3_0_0, context.getMinNodeVersion());
    }

    public void testGetSearchTaskId() {
        TaskId searchTaskId = new TaskId("node", 7);
        IndexAwareContext context = new IndexAwareContext(
            Collections.emptyMap(),
            new IndexMappingSourceCache(),
            MapperPlugin.NOOP_FIELD_FILTER,
            Version.CURRENT,
            searchTaskId,
            true
        );

        assertEquals(searchTaskId, context.getSearchTaskId());
    }

    private static IndexAwareContext contextFor(Map<String, IndexMetadata> targetIndices, IndexMappingSourceCache cache) {
        return new IndexAwareContext(targetIndices, cache, MapperPlugin.NOOP_FIELD_FILTER, Version.CURRENT, TaskId.EMPTY_TASK_ID, true);
    }

    private static IndexAwareContext contextAllowingPartialResults(boolean allowPartialSearchResults) {
        return new IndexAwareContext(
            Collections.emptyMap(),
            new IndexMappingSourceCache(),
            MapperPlugin.NOOP_FIELD_FILTER,
            Version.CURRENT,
            TaskId.EMPTY_TASK_ID,
            allowPartialSearchResults
        );
    }

    private static IndexMetadata indexWithMapping(String name, Map<String, Object> mapping) {
        return IndexMetadata.builder(name)
            // A UUID of its own, as every real index has: the mapping cache is keyed by it.
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid"))
            .putMapping(new MappingMetadata("_doc", mapping))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
