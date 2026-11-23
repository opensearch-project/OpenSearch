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
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

public class IndexMappingSourceCacheTests extends OpenSearchTestCase {

    private static final String UUID = "the-same-index-uuid";

    public void testReusesTheSameParsedMapAcrossSeparateLookups() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata metadata = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));

        // Two separate calls -- standing in for two separate requests, or two processors in one request --
        // for the same (index UUID, mapping version) must not re-parse: same cached instance back.
        Map<String, Object> first = cache.getMappingSourceAsMap(metadata);
        Map<String, Object> second = cache.getMappingSourceAsMap(metadata);
        assertSame(first, second);
    }

    public void testDifferentMetadataInstanceSameVersionStillHits() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        Map<String, Object> mapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        // Two distinct IndexMetadata objects (as resolveIndices() would build fresh per request), but the
        // same index UUID and mapping version -- i.e. the mapping hasn't actually changed between requests.
        IndexMetadata first = indexAt(UUID, 1, mapping);
        IndexMetadata second = indexAt(UUID, 1, mapping);

        Map<String, Object> firstResult = cache.getMappingSourceAsMap(first);
        Map<String, Object> secondResult = cache.getMappingSourceAsMap(second);
        assertSame(firstResult, secondResult);
    }

    public void testNewerMappingVersionIsParsedInsteadOfServedFromTheOlderEntry() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getMappingSourceAsMap(v1);

        Map<String, Object> resultV2 = cache.getMappingSourceAsMap(v2);

        assertEquals("keyword", titleType(resultV2));
    }

    // One entry per index, not one per mapping version: the older version's map is not kept.
    public void testNewerMappingVersionReplacesTheIndexEntry() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getMappingSourceAsMap(v1);

        cache.getMappingSourceAsMap(v2);

        assertEquals(1, cache.size());
    }

    public void testDifferentIndexUuidSameVersionIsACacheMiss() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        Map<String, Object> mapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        IndexMetadata indexA = indexAt("uuid-a", 1, mapping);
        IndexMetadata indexB = indexAt("uuid-b", 1, mapping);

        Map<String, Object> resultA = cache.getMappingSourceAsMap(indexA);
        Map<String, Object> resultB = cache.getMappingSourceAsMap(indexB);
        assertNotSame(resultA, resultB);
        assertEquals(2, cache.size());
    }

    // Metadata built without an index UUID all carries the same placeholder UUID. Caching by it would
    // hand one index's mapping to another, so such metadata is parsed every time and never cached.
    public void testIndicesWithoutAUuidAreNotConfusedWithEachOther() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata text = indexWithoutUuid("text-index", Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata keyword = indexWithoutUuid("keyword-index", Map.of("properties", Map.of("title", Map.of("type", "keyword"))));

        Map<String, Object> textResult = cache.getMappingSourceAsMap(text);
        Map<String, Object> keywordResult = cache.getMappingSourceAsMap(keyword);

        assertEquals("text", titleType(textResult));
        assertEquals("keyword", titleType(keywordResult));
        assertEquals(0, cache.size());
    }

    public void testNoMappingIsEmpty() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata noMapping = IndexMetadata.builder("no-mapping-index")
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, UUID))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        assertEquals(Collections.emptyMap(), cache.getMappingSourceAsMap(noMapping));
        assertEquals(0, cache.size());
    }

    // A request that still holds an older cluster state than one the cache has seen gets the mapping of
    // its own version, not the newer one that is cached.
    public void testOlderVersionGetsItsOwnMappingWhenANewerOneIsCached() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getMappingSourceAsMap(v2);

        Map<String, Object> resultV1 = cache.getMappingSourceAsMap(v1);

        assertEquals("text", titleType(resultV1));
    }

    // Serving that older request does not displace the newer entry everyone else is using.
    public void testOlderVersionDoesNotEvictANewerOne() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        Map<String, Object> resultV2 = cache.getMappingSourceAsMap(v2);

        cache.getMappingSourceAsMap(v1);

        assertSame(resultV2, cache.getMappingSourceAsMap(v2));
    }

    public void testConcurrentLookupsShareOneParsedMap() throws Exception {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata metadata = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        int threads = 8;
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Map<String, Object>> results = new CopyOnWriteArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
                for (int j = 0; j < 100; j++) {
                    results.add(cache.getMappingSourceAsMap(metadata));
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertEquals(threads * 100, results.size());
        Map<String, Object> first = results.get(0);
        for (Map<String, Object> result : results) {
            assertSame(first, result);
        }
        assertEquals(1, cache.size());
    }

    public void testRemoveDropsADeletedIndex() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata metadata = indexAt(UUID, 1, Map.of("properties", Map.of()));
        cache.getMappingSourceAsMap(metadata);
        assertEquals(1, cache.size());

        cache.remove(metadata.getIndex());
        assertEquals(0, cache.size());
    }

    // A cached map is shared by every request on the node, so no level of it may be modified. The four
    // tests below cover the map itself, a map nested in it, a list nested in it, and a map inside a list.

    public void testCachedMapIsUnmodifiable() {
        Map<String, Object> source = new IndexMappingSourceCache().getMappingSourceAsMap(indexWithNestedMapping());

        expectThrows(UnsupportedOperationException.class, () -> source.put("x", "y"));
    }

    public void testMapNestedInACachedMapIsUnmodifiable() {
        Map<String, Object> source = new IndexMappingSourceCache().getMappingSourceAsMap(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) source.get("properties");

        expectThrows(UnsupportedOperationException.class, () -> properties.put("x", "y"));
    }

    public void testListNestedInACachedMapIsUnmodifiable() {
        Map<String, Object> source = new IndexMappingSourceCache().getMappingSourceAsMap(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        List<Object> dynamicTemplates = (List<Object>) source.get("dynamic_templates");

        expectThrows(UnsupportedOperationException.class, () -> dynamicTemplates.add("x"));
    }

    public void testMapInsideAListOfACachedMapIsUnmodifiable() {
        Map<String, Object> source = new IndexMappingSourceCache().getMappingSourceAsMap(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        Map<String, Object> template = (Map<String, Object>) ((List<Object>) source.get("dynamic_templates")).get(0);

        expectThrows(UnsupportedOperationException.class, () -> template.put("x", "y"));
    }

    /** The {@code type} of the {@code title} field in a parsed mapping. */
    private static Object titleType(Map<String, Object> mappingSource) {
        return ((Map<?, ?>) ((Map<?, ?>) mappingSource.get("properties")).get("title")).get("type");
    }

    /** An index whose mapping has a nested map ({@code properties}) and a list of maps ({@code dynamic_templates}). */
    private static IndexMetadata indexWithNestedMapping() {
        return indexAt(
            UUID,
            1,
            Map.of(
                "properties",
                Map.of("title", Map.of("type", "text")),
                "dynamic_templates",
                List.of(Map.of("strings", Map.of("match_mapping_type", "string")))
            )
        );
    }

    private static IndexMetadata indexWithoutUuid(String name, Map<String, Object> mapping) {
        return IndexMetadata.builder(name)
            .settings(settings(Version.CURRENT))
            .putMapping(new MappingMetadata("_doc", mapping))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    private static IndexMetadata indexAt(String indexUuid, long mappingVersion, Map<String, Object> mapping) {
        return IndexMetadata.builder("index-" + indexUuid)
            .settings(settings(Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, indexUuid))
            .mappingVersion(mappingVersion)
            .putMapping(new MappingMetadata("_doc", mapping))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
