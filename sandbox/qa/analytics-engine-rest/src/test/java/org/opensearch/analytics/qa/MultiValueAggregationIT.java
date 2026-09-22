/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * End-to-end aggregation coverage for {@code multi_value: true} keyword fields.
 *
 * <p>Semantics locked in here:
 * <ul>
 *   <li>Grouping by a LIST field expands each document into one row per <em>distinct</em> element
 *       (Lucene terms-aggregation semantics: a document with {@code [blue, blue, red]} contributes
 *       once to {@code blue}). An empty array contributes no row; an absent field groups under
 *       {@code null}, exactly like a null scalar key.</li>
 *   <li>Explicit {@code mvexpand} does <em>not</em> deduplicate: {@code [blue, blue, red]} yields
 *       three rows. An absent field yields a single {@code null} row; an empty array yields none.</li>
 * </ul>
 *
 * <p>Multi-shard grouping by a LIST key, {@code list()}/{@code values()} over a LIST input, and
 * {@code dc()} over a LIST are known gaps and are pinned with {@link AwaitsFix} so the suite
 * documents them until the linked issues are fixed.
 */
public class MultiValueAggregationIT extends MultiValueRestTestCase {

    private static final String MULTI_SHARD_GROUP_BY_ISSUE = "https://github.com/opensearch-project/OpenSearch/issues/23057";
    private static final String LIST_VALUES_OVER_LIST_ISSUE = "https://github.com/opensearch-project/OpenSearch/issues/23058";
    private static final String DC_OVER_LIST_ISSUE = "https://github.com/opensearch-project/OpenSearch/issues/23059";

    /** Per-element document counts implied by the fixture (doc 1's duplicate {@code blue} counts once). */
    private static final Map<String, Long> EXPECTED_TAG_COUNTS = new TreeMap<>(
        Map.of("blue", 2L, "red", 2L, "green", 1L, UNICODE_A, 1L, UNICODE_B, 1L)
    );

    // ---- implicit expansion on GROUP BY (one shard) -----------------------------------------

    public void testCountByListFieldOneShard() throws Exception {
        Map<String, Number> groups = groups(
            executePpl("source = " + ONE_SHARD_INDEX + " | stats count() as cnt by tags"),
            "cnt",
            "tags"
        );
        assertEquals(EXPECTED_TAG_COUNTS, nonNullCounts(groups));
        // The empty-array document contributes no row; the absent-field document groups under null.
        assertEquals("absent-field document must form exactly one null bucket: " + groups, 1L, groups.get("null").longValue());
    }

    public void testCountByListAndScalarKeysOneShard() throws Exception {
        Map<String, Number> groups = groups(
            executePpl("source = " + ONE_SHARD_INDEX + " | stats count() as cnt by tags, region"),
            "cnt",
            "tags",
            "region"
        );
        Map<String, Long> expected = new TreeMap<>(
            Map.of("blue|us", 2L, "red|us", 1L, "red|eu", 1L, "green|eu", 1L, UNICODE_A + "|ap", 1L, UNICODE_B + "|ap", 1L)
        );
        assertEquals(expected, nonNullCounts(groups));
        assertEquals("absent-field document groups under a null tag with its region", 1L, groups.get("null|us").longValue());
    }

    public void testSumByListFieldOneShard() throws Exception {
        // Each distinct element row carries its document's latency: blue = 10 + 30, red = 10 + 20.
        Map<String, Number> groups = groups(
            executePpl("source = " + ONE_SHARD_INDEX + " | stats sum(latency) as total by tags"),
            "total",
            "tags"
        );
        assertEquals(
            new TreeMap<>(Map.of("blue", 40L, "red", 30L, "green", 20L, UNICODE_A, 60L, UNICODE_B, 60L)),
            nonNullCounts(groups)
        );
    }

    public void testAvgByListFieldOneShard() throws Exception {
        Map<String, Number> groups = groups(
            executePpl("source = " + ONE_SHARD_INDEX + " | stats avg(latency) as mean by tags"),
            "mean",
            "tags"
        );
        assertEquals(20.0, groups.get("blue").doubleValue(), 1e-9);
        assertEquals(15.0, groups.get("red").doubleValue(), 1e-9);
        assertEquals(20.0, groups.get("green").doubleValue(), 1e-9);
        assertEquals(60.0, groups.get(UNICODE_A).doubleValue(), 1e-9);
    }

    public void testSqlGroupByListFieldOneShard() throws Exception {
        Map<String, Number> groups = groups(
            executeSql("SELECT tags, COUNT(*) AS cnt FROM " + ONE_SHARD_INDEX + " GROUP BY tags"),
            "cnt",
            "tags"
        );
        assertEquals(EXPECTED_TAG_COUNTS, nonNullCounts(groups));
    }

    // ---- explicit expansion -----------------------------------------------------------------

    public void testMvexpandThenCountAll() throws Exception {
        // 3 + 2 + 1 + 0 (empty array) + 1 (absent field -> single null row) + 2 = 9 rows.
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | mvexpand tags | stats count() as cnt");
        assertEquals(9L, ((Number) rows(result).get(0).get(column(result, "cnt"))).longValue());
    }

    public void testMvexpandThenCountAllOneShardAgrees() throws Exception {
        Map<String, Object> one = executePpl("source = " + ONE_SHARD_INDEX + " | mvexpand tags | stats count() as cnt");
        Map<String, Object> two = executePpl("source = " + TWO_SHARD_INDEX + " | mvexpand tags | stats count() as cnt");
        assertEquals(rows(one).get(0).get(column(one, "cnt")), rows(two).get(0).get(column(two, "cnt")));
    }

    public void testMvexpandThenStatsByOneShardKeepsDuplicates() throws Exception {
        // Explicit expansion does not deduplicate within a document, unlike implicit GROUP BY.
        Map<String, Number> explicit = groups(
            executePpl("source = " + ONE_SHARD_INDEX + " | mvexpand tags | stats count() as cnt by tags"),
            "cnt",
            "tags"
        );
        assertEquals(
            new TreeMap<>(Map.of("blue", 3L, "red", 2L, "green", 1L, UNICODE_A, 1L, UNICODE_B, 1L)),
            nonNullCounts(explicit)
        );
    }

    // ---- known gaps: multi-shard GROUP BY on a LIST key --------------------------------------

    @AwaitsFix(bugUrl = MULTI_SHARD_GROUP_BY_ISSUE)
    public void testCountByListFieldTwoShards() throws Exception {
        Map<String, Number> groups = groups(
            executePpl("source = " + TWO_SHARD_INDEX + " | stats count() as cnt by tags"),
            "cnt",
            "tags"
        );
        assertEquals(EXPECTED_TAG_COUNTS, nonNullCounts(groups));
    }

    @AwaitsFix(bugUrl = MULTI_SHARD_GROUP_BY_ISSUE)
    public void testSqlGroupByListAndScalarTwoShards() throws Exception {
        Map<String, Number> groups = groups(
            executeSql("SELECT tags, region, SUM(latency) AS total FROM " + TWO_SHARD_INDEX + " GROUP BY tags, region"),
            "total",
            "tags",
            "region"
        );
        assertEquals(
            new TreeMap<>(Map.of("blue|us", 40L, "red|us", 10L, "red|eu", 20L, "green|eu", 20L, UNICODE_A + "|ap", 60L, UNICODE_B + "|ap", 60L)),
            nonNullCounts(groups)
        );
    }

    @AwaitsFix(bugUrl = MULTI_SHARD_GROUP_BY_ISSUE)
    public void testOneShardAndTwoShardAggregatesAgree() throws Exception {
        List<String> queries = List.of(
            " | stats count() as v by tags",
            " | stats sum(latency) as v by tags, region",
            " | mvexpand tags | stats count() as v by tags"
        );
        for (String ppl : queries) {
            Map<String, Number> one = groups(executePpl("source = " + ONE_SHARD_INDEX + ppl), "v", keyColumns(ppl));
            Map<String, Number> two = groups(executePpl("source = " + TWO_SHARD_INDEX + ppl), "v", keyColumns(ppl));
            assertEquals(ppl, nonNullCounts(one), nonNullCounts(two));
        }
    }

    // ---- known gaps: LIST-valued aggregate inputs -------------------------------------------

    @AwaitsFix(bugUrl = LIST_VALUES_OVER_LIST_ISSUE)
    public void testListAggregateOverListFieldFlattens() throws Exception {
        Map<String, Object> result = executePpl("source = " + ONE_SHARD_INDEX + " | stats list(tags) as all_tags");
        List<String> all = strings(rows(result).get(0).get(column(result, "all_tags")));
        assertEquals(sorted(List.of("blue", "blue", "red", "red", "green", "blue", UNICODE_A, UNICODE_B)), sorted(all));
    }

    @AwaitsFix(bugUrl = LIST_VALUES_OVER_LIST_ISSUE)
    public void testValuesAggregateOverListFieldIsDistinct() throws Exception {
        Map<String, Object> result = executePpl("source = " + ONE_SHARD_INDEX + " | stats values(tags) as distinct_tags");
        List<String> distinct = strings(rows(result).get(0).get(column(result, "distinct_tags")));
        assertEquals(sorted(List.of("blue", "red", "green", UNICODE_A, UNICODE_B)), sorted(distinct));
    }

    @AwaitsFix(bugUrl = LIST_VALUES_OVER_LIST_ISSUE)
    public void testListAggregateGroupedByScalar() throws Exception {
        Map<String, Object> result = executePpl("source = " + ONE_SHARD_INDEX + " | stats list(tags) as region_tags by region");
        int keyColumn = column(result, "region");
        int valueColumn = column(result, "region_tags");
        Map<String, List<String>> byRegion = new TreeMap<>();
        for (List<Object> row : rows(result)) {
            Object cell = row.get(valueColumn);
            byRegion.put((String) row.get(keyColumn), cell == null ? List.of() : sorted(strings(cell)));
        }
        assertEquals(sorted(List.of("blue", "blue", "red", "blue")), byRegion.get("us"));
        assertEquals(sorted(List.of("red", "green")), byRegion.get("eu"));
        assertEquals(sorted(List.of(UNICODE_A, UNICODE_B)), byRegion.get("ap"));
    }

    @AwaitsFix(bugUrl = DC_OVER_LIST_ISSUE)
    public void testDistinctCountOfListField() throws Exception {
        Map<String, Object> result = executePpl("source = " + ONE_SHARD_INDEX + " | stats dc(tags) as distinct_tags");
        List<List<Object>> rows = rows(result);
        assertEquals(1, rows.size());
        assertEquals(5L, ((Number) rows.get(0).get(column(result, "distinct_tags"))).longValue());
    }

    private static String[] keyColumns(String ppl) {
        String by = ppl.substring(ppl.lastIndexOf(" by ") + 4).trim();
        List<String> keys = new ArrayList<>();
        for (String key : by.split(",")) {
            keys.add(key.trim());
        }
        return keys.toArray(new String[0]);
    }
}
