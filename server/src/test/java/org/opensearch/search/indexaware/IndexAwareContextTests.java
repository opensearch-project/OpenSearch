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

    public void testGetFieldMappings() {
        Map<String, Object> fields = Map.of("title", Map.of("type", "text"));
        IndexAwareContext context = contextFor(
            Map.of("my-index", indexWithMapping("my-index", Map.of("properties", fields))),
            new IndexMappingSourceCache()
        );

        assertEquals(fields, context.getFieldMappings("my-index"));
    }

    // Only the properties section: the rest of the mapping is in the index's IndexMetadata.
    public void testGetFieldMappingsLeavesOutTheRestOfTheMapping() {
        Map<String, Object> fields = Map.of("title", Map.of("type", "text"));
        Map<String, Object> mapping = Map.of("dynamic", "strict", "_meta", Map.of("owner", "search"), "properties", fields);
        IndexAwareContext context = contextFor(Map.of("my-index", indexWithMapping("my-index", mapping)), new IndexMappingSourceCache());

        assertEquals(fields, context.getFieldMappings("my-index"));
    }

    // Two indices behind one request that map the same field differently: each index reports its own
    // mapping, so a processor can tell them apart.
    public void testGetFieldMappingsIsPerIndex() {
        Map<String, Object> textFields = Map.of("title", Map.of("type", "text"));
        Map<String, Object> keywordFields = Map.of("title", Map.of("type", "keyword"));
        IndexAwareContext context = contextFor(
            Map.of(
                "text-index",
                indexWithMapping("text-index", Map.of("properties", textFields)),
                "keyword-index",
                indexWithMapping("keyword-index", Map.of("properties", keywordFields))
            ),
            new IndexMappingSourceCache()
        );

        assertEquals(textFields, context.getFieldMappings("text-index"));
        assertEquals(keywordFields, context.getFieldMappings("keyword-index"));
    }

    // Two contexts stand in for two requests. Backed by the same node-level cache, the second must get
    // the map the first one parsed.
    public void testGetFieldMappingsIsSharedAcrossContexts() {
        IndexMetadata metadata = indexWithMapping("my-index", Map.of("properties", Map.of()));
        IndexMappingSourceCache sharedCache = new IndexMappingSourceCache();
        IndexAwareContext firstRequestContext = contextFor(Map.of("my-index", metadata), sharedCache);
        IndexAwareContext secondRequestContext = contextFor(Map.of("my-index", metadata), sharedCache);

        Map<String, Object> first = firstRequestContext.getFieldMappings("my-index");
        Map<String, Object> second = secondRequestContext.getFieldMappings("my-index");
        assertSame(first, second);
    }

    public void testGetFieldMappingsUnknownIndexIsEmpty() {
        IndexAwareContext context = contextFor(Collections.emptyMap(), new IndexMappingSourceCache());

        assertEquals(Collections.emptyMap(), context.getFieldMappings("not-a-target-index"));
    }

    public void testGetFieldMappingsNoMappingIsEmpty() {
        IndexMetadata metadata = IndexMetadata.builder("my-index")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        IndexAwareContext context = contextFor(Map.of("my-index", metadata), new IndexMappingSourceCache());

        assertNull(metadata.mapping());
        assertEquals(Collections.emptyMap(), context.getFieldMappings("my-index"));
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

    // The fields a processor reads are all of the index's fields, whatever the field filter hides: hiding
    // fields from the caller is the processor's job, through getFieldFilter.
    public void testGetFieldMappingsIsNotReducedByTheFieldFilter() {
        Map<String, Object> fields = Map.of("secret", Map.of("type", "text"));
        IndexAwareContext context = new IndexAwareContext(
            Map.of("my-index", indexWithMapping("my-index", Map.of("properties", fields))),
            new IndexMappingSourceCache(),
            index -> field -> false,
            Version.CURRENT,
            TaskId.EMPTY_TASK_ID,
            true
        );

        assertEquals(fields, context.getFieldMappings("my-index"));
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
