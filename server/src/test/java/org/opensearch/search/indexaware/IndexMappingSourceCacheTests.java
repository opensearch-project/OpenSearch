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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;

public class IndexMappingSourceCacheTests extends OpenSearchTestCase {

    private static final String UUID = "the-same-index-uuid";

    public void testReusesTheSameParsedMapAcrossSeparateLookups() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata metadata = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));

        // Two separate calls -- standing in for two separate requests, or two processors in one request --
        // for the same (index UUID, mapping version) must not re-parse: same cached instance back.
        Map<String, Object> first = cache.getFieldMappings(metadata);
        Map<String, Object> second = cache.getFieldMappings(metadata);
        assertSame(first, second);
    }

    public void testDifferentMetadataInstanceSameVersionStillHits() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        Map<String, Object> mapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        // Two distinct IndexMetadata objects (as resolveIndices() would build fresh per request), but the
        // same index UUID and mapping version -- i.e. the mapping hasn't actually changed between requests.
        IndexMetadata first = indexAt(UUID, 1, mapping);
        IndexMetadata second = indexAt(UUID, 1, mapping);

        Map<String, Object> firstResult = cache.getFieldMappings(first);
        Map<String, Object> secondResult = cache.getFieldMappings(second);
        assertSame(firstResult, secondResult);
    }

    public void testNewerMappingVersionIsParsedInsteadOfServedFromTheOlderEntry() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getFieldMappings(v1);

        Map<String, Object> resultV2 = cache.getFieldMappings(v2);

        assertEquals("keyword", titleType(resultV2));
    }

    // One entry per index, not one per mapping version: the older version's map is not kept.
    public void testNewerMappingVersionReplacesTheIndexEntry() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getFieldMappings(v1);

        cache.getFieldMappings(v2);

        assertEquals(1, cache.size());
    }

    public void testDifferentIndexUuidSameVersionIsACacheMiss() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        Map<String, Object> mapping = Map.of("properties", Map.of("title", Map.of("type", "text")));
        IndexMetadata indexA = indexAt("uuid-a", 1, mapping);
        IndexMetadata indexB = indexAt("uuid-b", 1, mapping);

        Map<String, Object> resultA = cache.getFieldMappings(indexA);
        Map<String, Object> resultB = cache.getFieldMappings(indexB);
        assertNotSame(resultA, resultB);
        assertEquals(2, cache.size());
    }

    // Metadata built without an index UUID all carries the same placeholder UUID. Caching by it would
    // hand one index's mapping to another, so such metadata is parsed every time and never cached.
    public void testIndicesWithoutAUuidAreNotConfusedWithEachOther() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata text = indexWithoutUuid("text-index", Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata keyword = indexWithoutUuid("keyword-index", Map.of("properties", Map.of("title", Map.of("type", "keyword"))));

        Map<String, Object> textResult = cache.getFieldMappings(text);
        Map<String, Object> keywordResult = cache.getFieldMappings(keyword);

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

        assertEquals(Collections.emptyMap(), cache.getFieldMappings(noMapping));
        assertEquals(0, cache.size());
    }

    // A request that still holds an older cluster state than one the cache has seen gets the mapping of
    // its own version, not the newer one that is cached.
    public void testOlderVersionGetsItsOwnMappingWhenANewerOneIsCached() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        cache.getFieldMappings(v2);

        Map<String, Object> resultV1 = cache.getFieldMappings(v1);

        assertEquals("text", titleType(resultV1));
    }

    // Serving that older request does not displace the newer entry everyone else is using.
    public void testOlderVersionDoesNotEvictANewerOne() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache();
        IndexMetadata v1 = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text"))));
        IndexMetadata v2 = indexAt(UUID, 2, Map.of("properties", Map.of("title", Map.of("type", "keyword"))));
        Map<String, Object> resultV2 = cache.getFieldMappings(v2);

        cache.getFieldMappings(v1);

        assertSame(resultV2, cache.getFieldMappings(v2));
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
                    results.add(cache.getFieldMappings(metadata));
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
        cache.getFieldMappings(metadata);
        assertEquals(1, cache.size());

        cache.remove(metadata.getIndex());
        assertEquals(0, cache.size());
    }

    // A cached map is shared by every request on the node, so no level of it may be modified. The four
    // tests below cover the map itself, a map nested in it, a list nested in it, and a map inside a list.

    public void testCachedMapIsUnmodifiable() {
        Map<String, Object> fields = new IndexMappingSourceCache().getFieldMappings(indexWithNestedMapping());

        expectThrows(UnsupportedOperationException.class, () -> fields.put("x", "y"));
    }

    public void testMapNestedInACachedMapIsUnmodifiable() {
        Map<String, Object> fields = new IndexMappingSourceCache().getFieldMappings(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        Map<String, Object> title = (Map<String, Object>) fields.get("title");

        expectThrows(UnsupportedOperationException.class, () -> title.put("x", "y"));
    }

    public void testListNestedInACachedMapIsUnmodifiable() {
        Map<String, Object> fields = new IndexMappingSourceCache().getFieldMappings(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        List<Object> copyTo = (List<Object>) ((Map<String, Object>) fields.get("title")).get("copy_to");

        expectThrows(UnsupportedOperationException.class, () -> copyTo.add("x"));
    }

    public void testMapInsideAListOfACachedMapIsUnmodifiable() {
        Map<String, Object> fields = new IndexMappingSourceCache().getFieldMappings(indexWithNestedMapping());
        @SuppressWarnings("unchecked")
        Map<String, Object> rule = (Map<String, Object>) ((List<Object>) ((Map<String, Object>) fields.get("embedding")).get("rules")).get(
            0
        );

        expectThrows(UnsupportedOperationException.class, () -> rule.put("x", "y"));
    }

    // Only the properties section is cached: the rest of the mapping is not held in memory.
    public void testOnlyFieldMappingsAreCached() {
        Map<String, Object> fields = Map.of("title", Map.of("type", "text"));
        IndexMetadata metadata = indexAt(
            UUID,
            1,
            Map.of(
                "dynamic",
                "strict",
                "_meta",
                Map.of("owner", "search"),
                "dynamic_templates",
                List.of(Map.of("strings", Map.of("match_mapping_type", "string"))),
                "properties",
                fields
            )
        );

        assertEquals(fields, new IndexMappingSourceCache().getFieldMappings(metadata));
    }

    public void testMappingWithoutFieldsIsEmpty() {
        IndexMetadata metadata = indexAt(UUID, 1, Map.of("dynamic", "strict"));

        assertEquals(Collections.emptyMap(), new IndexMappingSourceCache().getFieldMappings(metadata));
    }

    // A size of 0 caches nothing, and every lookup still gets the right fields.
    public void testZeroSizeCachesNothing() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache(0);
        IndexMetadata metadata = indexAt(UUID, 1, titleOfType("keyword"));

        assertEquals("keyword", titleType(cache.getFieldMappings(metadata)));
        assertEquals(0, cache.size());
    }

    public void testSizeSettingSetsTheBound() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache(
            Settings.builder().put(IndexMappingSourceCache.SIZE_SETTING.getKey(), "0b").build()
        );

        cache.getFieldMappings(indexAt(UUID, 1, titleOfType("text")));

        assertEquals(0, cache.size());
    }

    public void testSizeSettingDefaultsToOnePercentOfTheHeap() {
        assertEquals(
            new ByteSizeValue(JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 100).getBytes(),
            IndexMappingSourceCache.SIZE_SETTING.get(Settings.EMPTY).getBytes(),
            1
        );
    }

    public void testEvictsBeyondTheSize() {
        long oneEntry = weightOfOneEntry();
        IndexMappingSourceCache cache = new IndexMappingSourceCache(2 * oneEntry);

        cache.getFieldMappings(indexAt("uuid-a", 1, titleOfType("text")));
        cache.getFieldMappings(indexAt("uuid-b", 1, titleOfType("text")));
        cache.getFieldMappings(indexAt("uuid-c", 1, titleOfType("text")));

        assertEquals(2, cache.size());
        assertTrue(cache.weight() <= 2 * oneEntry);
    }

    // The entry evicted is the one used least recently, not the one inserted first.
    public void testEvictsTheLeastRecentlyUsedIndex() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache(2 * weightOfOneEntry());
        IndexMetadata a = indexAt("uuid-a", 1, titleOfType("text"));
        IndexMetadata b = indexAt("uuid-b", 1, titleOfType("text"));
        IndexMetadata c = indexAt("uuid-c", 1, titleOfType("text"));
        cache.getFieldMappings(a);
        cache.getFieldMappings(b);
        cache.getFieldMappings(a);

        cache.getFieldMappings(c);

        assertFalse(cache.contains(b.getIndex()));
        assertTrue(cache.contains(a.getIndex()));
        assertTrue(cache.contains(c.getIndex()));
    }

    // Lucene's RamUsageEstimator#sizeOfObject looks one level into a map, so it would weigh a long value
    // three levels down as a few bytes. The cache's own estimate counts it.
    public void testWeightCountsDeeplyNestedValues() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache(Long.MAX_VALUE);
        String longValue = "x".repeat(10_000);

        cache.getFieldMappings(
            indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("fields", Map.of("raw", Map.of("meta", longValue))))))
        );

        assertTrue("weight " + cache.weight(), cache.weight() > longValue.length());
    }

    // An entry larger than the whole cache is not kept, but its caller still gets the fields.
    public void testEntryLargerThanTheCacheIsNotKept() {
        IndexMappingSourceCache cache = new IndexMappingSourceCache(weightOfOneEntry());
        IndexMetadata large = indexAt(UUID, 1, Map.of("properties", Map.of("title", Map.of("type", "text", "meta", "x".repeat(10_000)))));

        Map<String, Object> fields = cache.getFieldMappings(large);

        assertEquals("text", titleType(fields));
        assertEquals(0, cache.size());
    }

    // Many threads reading more indices than the cache holds: each gets its own index's fields, and the
    // cache stays within its size.
    public void testConcurrentLookupsBeyondTheSize() throws Exception {
        long maxWeight = 5 * weightOfOneEntry();
        int threads = 8;
        IndexMappingSourceCache cache = new IndexMappingSourceCache(maxWeight);
        List<IndexMetadata> indices = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            indices.add(indexAt("uuid-" + (char) ('a' + i), 1, titleOfType("type-" + (char) ('a' + i))));
        }
        CyclicBarrier start = new CyclicBarrier(threads);
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            // The test framework's Random belongs to the test thread: give each worker its own.
            Random random = new Random(randomLong());
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                    for (int j = 0; j < 500; j++) {
                        int i = random.nextInt(indices.size());
                        assertEquals("type-" + (char) ('a' + i), titleType(cache.getFieldMappings(indices.get(i))));
                    }
                } catch (Throwable e) {
                    failures.add(e);
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }
        assertEquals(Collections.emptyList(), failures);
        assertTrue("weight " + cache.weight(), cache.weight() <= maxWeight);
    }

    /** The {@code type} of the {@code title} field in a set of field mappings. */
    private static Object titleType(Map<String, Object> fieldMappings) {
        return ((Map<?, ?>) fieldMappings.get("title")).get("type");
    }

    /** The weight of one entry for a {@code uuid-?} index whose {@code title} is of a one-word type. */
    private static long weightOfOneEntry() {
        IndexMappingSourceCache probe = new IndexMappingSourceCache(Long.MAX_VALUE);
        probe.getFieldMappings(indexAt("uuid-z", 1, titleOfType("text")));
        return probe.weight();
    }

    /**
     * An index whose fields include a nested map ({@code title}), a list ({@code copy_to}) and a list of
     * maps ({@code rules}; no built-in field type has one, but a plugin's field type may).
     */
    private static IndexMetadata indexWithNestedMapping() {
        return indexAt(
            UUID,
            1,
            Map.of(
                "properties",
                Map.of(
                    "title",
                    Map.of("type", "text", "copy_to", List.of("all_text")),
                    "embedding",
                    Map.of("type", "plugin_type", "rules", List.of(Map.of("when", "always")))
                )
            )
        );
    }

    private static Map<String, Object> titleOfType(String type) {
        return Map.of("properties", Map.of("title", Map.of("type", type)));
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
