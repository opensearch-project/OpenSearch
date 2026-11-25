/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.FieldType;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/** Exercises shared immutable mapper state through actual index services and transport actions. */
public class SharedMapperStateIT extends OpenSearchSingleNodeTestCase {
    private static final Settings INDEX_SETTINGS = Settings.builder()
        .put("index.number_of_shards", 1)
        .put("index.number_of_replicas", 0)
        .put("index.derived_source.enabled", true)
        .build();

    public void testFrozenPlaceholderIsSharedAcrossIndexServices() {
        final Map<String, Object> properties = new LinkedHashMap<>();
        final String[] types = { "text", "keyword", "long", "date", "boolean", "ip" };
        for (final String type : types) {
            for (int i = 0; i < 16; i++) {
                properties.put(type + i, Map.of("type", type));
            }
        }
        final IndexService first = createMappedIndex("first", properties);
        final IndexService second = createMappedIndex("second", properties);
        assertNotSame(first.mapperService(), second.mapperService());
        final FieldType shared = field(first, "keyword0").fieldType;
        assertEquals(new FieldType(), shared);
        assertNull(shared.getAttributes());
        expectThrows(IllegalStateException.class, () -> shared.setStored(true));
        for (final String name : properties.keySet()) {
            assertSame(name, shared, field(first, name).fieldType);
            assertSame(name, shared, field(second, name).fieldType);
            assertNotSame(field(first, name), field(second, name));
            assertNotSame(field(first, name).fieldType(), field(second, name).fieldType());
        }
    }

    public void testEmptyMultiFieldsIsSharedAfterMappingMerge() {
        final IndexService first = createMappedIndex("first", Map.of("tenant", Map.of("type", "keyword")));
        final IndexService second = createMappedIndex("second", Map.of("tenant", Map.of("type", "keyword")));
        final FieldMapper.MultiFields empty = FieldMapper.MultiFields.empty();
        assertSame(empty, FieldMapper.MultiFields.empty());
        assertSame(empty, field(first, "tenant").multiFields());
        assertSame(empty, field(second, "tenant").multiFields());
        assertSame(empty, empty.merge(empty));
        assertFalse(empty.iterator().hasNext());
        assertAcked(
            client().admin()
                .indices()
                .preparePutMapping("first")
                .setSource(
                    Map.of(
                        "properties",
                        Map.of("tenant", Map.of("type", "keyword", "eager_global_ordinals", true), "new_field", Map.of("type", "long"))
                    )
                )
        );
        assertSame(empty, field(first, "tenant").multiFields());
        assertSame(empty, field(first, "new_field").multiFields());
        assertTrue(field(first, "tenant").fieldType().eagerGlobalOrdinals());
        assertFalse(field(second, "tenant").fieldType().eagerGlobalOrdinals());
        assertNull(second.mapperService().documentMapper().mappers().getMapper("new_field"));
        assertSame(empty, field(second, "tenant").multiFields());
        // Adding the first child must leave the shared empty instance and the peer index unchanged.
        assertAcked(
            client().admin()
                .indices()
                .preparePutMapping("first")
                .setSource(
                    Map.of("properties", Map.of("tenant", Map.of("type", "keyword", "fields", Map.of("exact", Map.of("type", "keyword")))))
                )
        );
        assertNotSame(empty, field(first, "tenant").multiFields());
        assertSame(empty, field(first, "tenant.exact").multiFields());
        assertSame(empty, field(second, "tenant").multiFields());
        assertNull(second.mapperService().documentMapper().mappers().getMapper("tenant.exact"));
        client().prepareIndex("first").setId("1").setSource("tenant", "t1").setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("second").setId("1").setSource("tenant", "t1").setRefreshPolicy(IMMEDIATE).get();
        assertHits("first", "tenant.exact", "t1", 1);
        assertHits("second", "tenant", "t1", 1);
    }

    public void testMappingUpdatesAndIndexLifecyclePreserveSearchableData() {
        final Map<String, Object> properties = Map.of(
            "tenant",
            Map.of("type", "keyword"),
            "title",
            Map.of("type", "text", "analyzer", "whitespace", "fields", Map.of("raw", Map.of("type", "keyword"))),
            "count",
            Map.of("type", "long"),
            "timestamp",
            Map.of("type", "date", "format", "strict_date_optional_time||epoch_millis")
        );
        final IndexService first = createMappedIndex("first", properties);
        final IndexService second = createMappedIndex("second", properties);
        final Map<String, Object> document = Map.of(
            "tenant",
            "t1",
            "title",
            "Alpha Beta",
            "count",
            42,
            "timestamp",
            "2026-01-01T00:00:00Z"
        );
        client().prepareIndex("first").setId("1").setSource(document).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("second").setId("1").setSource(document).setRefreshPolicy(IMMEDIATE).get();
        assertHits("first", "title.raw", "Alpha Beta", 1);
        assertHits("second", "title", "Alpha", 1);
        assertAcked(
            client().admin()
                .indices()
                .preparePutMapping("first")
                .setSource(
                    Map.of(
                        "properties",
                        Map.of(
                            "title",
                            Map.of(
                                "type",
                                "text",
                                "analyzer",
                                "whitespace",
                                "fields",
                                Map.of("folded", Map.of("type", "text", "analyzer", "standard"))
                            )
                        )
                    )
                )
        );
        client().prepareIndex("first").setId("2").setSource(document).setRefreshPolicy(IMMEDIATE).get();
        assertHits("first", "title.raw", "Alpha Beta", 2);
        assertHits("first", "title.folded", "alpha", 1);
        assertNull(second.mapperService().documentMapper().mappers().getMapper("title.folded"));
        assertNotSame(field(first, "title").multiFields(), field(second, "title").multiFields());
        assertAcked(client().admin().indices().prepareClose("first"));
        assertHits("second", "title.raw", "Alpha Beta", 1);
        assertAcked(client().admin().indices().prepareOpen("first"));
        assertHits("first", "title.raw", "Alpha Beta", 2);
        final Map<String, Object> source = client().prepareGet("first", "1").get().getSourceAsMap();
        assertEquals("t1", source.get("tenant"));
        assertEquals("Alpha Beta", source.get("title"));
        assertEquals(42, ((Number) source.get("count")).intValue());
        assertAcked(client().admin().indices().prepareDelete("first"));
        final IndexService recreated = createMappedIndex("first", Map.of("title", Map.of("type", "keyword")));
        client().prepareIndex("first").setId("new").setSource("title", "Gamma Delta").setRefreshPolicy(IMMEDIATE).get();
        assertEquals("keyword", field(recreated, "title").fieldType().typeName());
        assertEquals("text", field(second, "title").fieldType().typeName());
        assertHits("first", "title", "Gamma Delta", 1);
        assertHits("first", "title", "Alpha Beta", 0);
        assertHits("second", "title", "Alpha", 1);
    }

    private IndexService createMappedIndex(final String name, final Map<String, Object> properties) {
        return createIndex(
            name,
            client().admin().indices().prepareCreate(name).setSettings(INDEX_SETTINGS).setMapping(Map.of("properties", properties))
        );
    }

    private static FieldMapper field(final IndexService service, final String name) {
        return (FieldMapper) service.mapperService().documentMapper().mappers().getMapper(name);
    }

    private void assertHits(final String index, final String field, final Object value, final long count) {
        final SearchResponse response = client().prepareSearch(index).setQuery(termQuery(field, value)).get();
        assertFalse(response.isTimedOut());
        assertEquals(0, response.getFailedShards());
        assertEquals(count, response.getHits().getTotalHits().value());
    }
}
