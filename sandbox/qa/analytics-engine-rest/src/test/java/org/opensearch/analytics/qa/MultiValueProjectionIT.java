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

/**
 * End-to-end projection coverage for {@code multi_value: true} keyword fields.
 *
 * <p>Every query runs through the production PPL/SQL frontends against the two-shard index (shard
 * fragments, Arrow FFI boundary, coordinator reduce) and, where noted, the one-shard index for
 * parity. Assertions compare exact LIST cells, so element order and in-document duplicates are
 * part of the contract.
 */
public class MultiValueProjectionIT extends MultiValueRestTestCase {

    // ---- direct LIST projection -------------------------------------------------------------

    public void testWideProjectionWithScalarSiblings() throws Exception {
        // Three columns of mixed scalar/LIST types: the exact shape that produced the LIST child-name
        // mismatch (`element` vs `item`) across the shard -> coordinator schema boundary.
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | fields id, tags, region | sort id");
        assertEquals(List.of("id", "tags", "region"), extractColumnNames(result));
        assertFixtureTags(tagsById(result));

        int idColumn = column(result, "id");
        int regionColumn = column(result, "region");
        Map<Integer, String> regions = new java.util.HashMap<>();
        for (List<Object> row : rows(result)) {
            regions.put(((Number) row.get(idColumn)).intValue(), (String) row.get(regionColumn));
        }
        assertEquals(Map.of(1, "us", 2, "eu", 3, "us", 4, "eu", 5, "us", 6, "ap"), regions);
    }

    public void testListOnlyProjection() throws Exception {
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | fields tags");
        assertEquals(List.of("tags"), extractColumnNames(result));
        int tagsColumn = column(result, "tags");

        List<String> rendered = new ArrayList<>();
        int absent = 0;
        for (List<Object> row : rows(result)) {
            Object cell = row.get(tagsColumn);
            if (cell == null || strings(cell).isEmpty()) {
                absent++;
            } else {
                rendered.add(String.join(",", strings(cell)));
            }
        }
        assertEquals("empty-array and absent-field documents", 2, absent);
        assertEquals(
            sorted(List.of("blue,blue,red", "red,green", "blue", UNICODE_A + "," + UNICODE_B)),
            sorted(rendered)
        );
    }

    public void testDuplicatesAndElementOrderArePreserved() throws Exception {
        Map<Integer, Object> byId = tagsById(executePpl("source = " + TWO_SHARD_INDEX + " | fields id, tags"));
        assertEquals("in-document duplicate must survive with its position", List.of("blue", "blue", "red"), strings(byId.get(1)));
        assertEquals("stored element order must be preserved", List.of("red", "green"), strings(byId.get(2)));
    }

    public void testNonAsciiElementsRoundTrip() throws Exception {
        Map<Integer, Object> byId = tagsById(executePpl("source = " + TWO_SHARD_INDEX + " | fields id, tags"));
        assertEquals(List.of(UNICODE_A, UNICODE_B), strings(byId.get(6)));
    }

    public void testEmptyArrayAndAbsentFieldRendering() throws Exception {
        Map<Integer, Object> byId = tagsById(executePpl("source = " + TWO_SHARD_INDEX + " | fields id, tags"));
        assertEquals("explicit empty array must project as an empty LIST", List.of(), strings(byId.get(4)));
        assertAbsentCell(byId.get(5));
    }

    public void testSingleElementIsStillAList() throws Exception {
        Map<Integer, Object> byId = tagsById(executePpl("source = " + TWO_SHARD_INDEX + " | fields id, tags"));
        assertEquals("single-element documents must not collapse to a scalar", List.of("blue"), strings(byId.get(3)));
    }

    // ---- LIST projection through other operators --------------------------------------------

    public void testScalarFilterThenListProjection() throws Exception {
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | where region = 'us' | fields id, tags");
        Map<Integer, Object> byId = tagsById(result);
        assertEquals("us documents", java.util.Set.of(1, 3, 5), byId.keySet());
        assertEquals(List.of("blue", "blue", "red"), strings(byId.get(1)));
        assertEquals(List.of("blue"), strings(byId.get(3)));
        assertAbsentCell(byId.get(5));
    }

    public void testSortHeadThenListProjection() throws Exception {
        // sort + head + fields is the query-then-fetch (late materialization) shape: the LIST column is
        // fetched by doc id after the top-N is decided, so it crosses the fetch boundary as a payload.
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | sort - latency | head 3 | fields id, tags");
        int idColumn = column(result, "id");
        List<Integer> ids = new ArrayList<>();
        for (List<Object> row : rows(result)) {
            ids.add(((Number) row.get(idColumn)).intValue());
        }
        assertEquals("top-3 by latency descending", List.of(6, 5, 4), ids);

        Map<Integer, Object> byId = tagsById(result);
        assertEquals(List.of(UNICODE_A, UNICODE_B), strings(byId.get(6)));
        assertAbsentCell(byId.get(5));
        assertEquals(List.of(), strings(byId.get(4)));
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/23061")
    public void testSortAscendingHeadThenListProjection() throws Exception {
        // Placement-dependent: with some shard splits the fetched LIST for doc 2 comes back empty.
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | sort latency | head 2 | fields id, tags");
        Map<Integer, Object> byId = tagsById(result);
        assertEquals(java.util.Set.of(1, 2), byId.keySet());
        assertEquals(List.of("blue", "blue", "red"), strings(byId.get(1)));
        assertEquals(result.toString(), List.of("red", "green"), strings(byId.get(2)));
    }

    // ---- explicit row expansion -------------------------------------------------------------

    public void testMvexpandProducesOneRowPerElement() throws Exception {
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | mvexpand tags | fields id, tags");
        int idColumn = column(result, "id");
        int tagsColumn = column(result, "tags");
        List<String> pairs = new ArrayList<>();
        for (List<Object> row : rows(result)) {
            Object tag = row.get(tagsColumn);
            assertFalse("mvexpand must yield scalar cells, got LIST in " + row, tag instanceof List);
            pairs.add(row.get(idColumn) + "=" + tag);
        }
        // Empty-array (4) contributes no rows; the absent-field document (5) passes through as one
        // null row (unnest preserve_nulls, matching Splunk mvexpand).
        assertEquals(
            sorted(
                List.of("1=blue", "1=blue", "1=red", "2=red", "2=green", "3=blue", "5=null", "6=" + UNICODE_A, "6=" + UNICODE_B)
            ),
            sorted(pairs)
        );
    }

    public void testMvexpandWithLimitKeepsLeadingElements() throws Exception {
        Map<String, Object> result = executePpl("source = " + TWO_SHARD_INDEX + " | mvexpand tags limit=1 | fields id, tags");
        int idColumn = column(result, "id");
        int tagsColumn = column(result, "tags");
        List<String> pairs = new ArrayList<>();
        for (List<Object> row : rows(result)) {
            pairs.add(row.get(idColumn) + "=" + row.get(tagsColumn));
        }
        assertEquals(sorted(List.of("1=blue", "2=red", "3=blue", "5=null", "6=" + UNICODE_A)), sorted(pairs));
    }

    // ---- SQL parity -------------------------------------------------------------------------

    public void testSqlListProjection() throws Exception {
        Map<String, Object> result = executeSql("SELECT id, tags FROM " + TWO_SHARD_INDEX + " ORDER BY id");
        assertFixtureTags(tagsById(result));
    }

    public void testSqlFilteredListProjection() throws Exception {
        Map<String, Object> result = executeSql("SELECT id, tags FROM " + TWO_SHARD_INDEX + " WHERE latency >= 20 ORDER BY id");
        Map<Integer, Object> byId = tagsById(result);
        assertEquals(java.util.Set.of(2, 3, 4, 5, 6), byId.keySet());
        assertEquals(List.of("red", "green"), strings(byId.get(2)));
        assertEquals(List.of(), strings(byId.get(4)));
    }

    // ---- single-shard parity ----------------------------------------------------------------

    public void testOneShardAndTwoShardProjectionsAgree() throws Exception {
        String ppl = " | fields id, tags, region | sort id";
        Map<Integer, Object> one = tagsById(executePpl("source = " + ONE_SHARD_INDEX + ppl));
        Map<Integer, Object> two = tagsById(executePpl("source = " + TWO_SHARD_INDEX + ppl));
        assertFixtureTags(one);
        assertFixtureTags(two);
        for (Integer id : EXPECTED_TAGS.keySet()) {
            assertEquals("id=" + id, strings(one.get(id)), strings(two.get(id)));
        }
    }

    public void testOneShardMvexpandAgreesWithTwoShard() throws Exception {
        String ppl = " | mvexpand tags | fields id, tags";
        assertEquals(expandedPairs(executePpl("source = " + ONE_SHARD_INDEX + ppl)), expandedPairs(executePpl("source = " + TWO_SHARD_INDEX + ppl)));
    }

    private static List<String> expandedPairs(Map<String, Object> result) {
        int idColumn = column(result, "id");
        int tagsColumn = column(result, "tags");
        List<String> pairs = new ArrayList<>();
        for (List<Object> row : rows(result)) {
            pairs.add(row.get(idColumn) + "=" + row.get(tagsColumn));
        }
        return sorted(pairs);
    }
}
