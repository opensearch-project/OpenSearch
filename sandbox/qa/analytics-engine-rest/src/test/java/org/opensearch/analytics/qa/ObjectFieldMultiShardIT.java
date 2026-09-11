/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Multi-shard coverage for grouping on an {@code object}. {@link ObjectFieldIT} exercises the same
 * queries at 1 shard, which cannot reach the PARTIAL/FINAL reduce path.
 *
 * <p>Grouping on a struct value goes through DataFusion's generic row-encoded group column
 * ({@code RowsGroupColumn}) rather than a per-type columnar builder, since {@code Struct} has no
 * specialization yet. That is a throughput question, not a correctness one — but the PARTIAL/FINAL
 * reduce it runs through is untested at 1 shard, so these pin the results it produces.
 *
 * <p>Reuses {@code object_fields}' mapping and bulk data under a distinct index name so the 1-shard
 * index in {@link ObjectFieldIT} is untouched.
 */
public class ObjectFieldMultiShardIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("object_fields", "object_fields_multishard");

    /** Seattle, Portland, Austin — one group per city in the dataset. */
    private static final int DISTINCT_CITIES = 3;

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), DATASET, 2);
            provisioned = true;
        }
    }

    /**
     * Grouping on the whole object across shards: one group per city, every group key returned as a
     * nested object (not flattened), and the per-shard counts reduced to the full document count.
     */
    @SuppressWarnings("unchecked")
    public void testGroupByTopLevelObjectFieldAtTwoShards() throws IOException {
        List<List<Object>> rows = rowsOf("source=" + DATASET.indexName + " | stats count() by city");
        assertEquals("one group per city", DISTINCT_CITIES, rows.size());

        long total = 0;
        for (List<Object> row : rows) {
            // stats output is [count, groupKey]; locate the map rather than assuming a position.
            Map<String, Object> city = null;
            for (Object cell : row) {
                if (cell instanceof Map<?, ?> map) {
                    city = (Map<String, Object>) map;
                } else if (cell instanceof Number n) {
                    total += n.longValue();
                }
            }
            assertNotNull("group key must be a nested object, got row: " + row, city);
            assertTrue("object must carry its leaves, got: " + city, city.containsKey("name") && city.containsKey("population"));
            assertTrue("sub-object must stay nested, got: " + city, city.get("location") instanceof Map);
        }
        assertEquals("reduced counts must cover every document", DISTINCT_CITIES, total);
    }

    /** An intermediate object as the group key — the nested make_struct path, across shards. */
    public void testGroupByIntermediateObjectFieldAtTwoShards() throws IOException {
        assertEquals(
            DISTINCT_CITIES,
            rowsOf("source=" + DATASET.indexName + " | stats count() by city.location").size()
        );
    }

    /** A leaf aggregate grouped by an object: agg call and struct group key reduced together. */
    public void testAggregateLeafGroupedByObjectFieldAtTwoShards() throws IOException {
        assertEquals(
            DISTINCT_CITIES,
            rowsOf("source=" + DATASET.indexName + " | stats max(city.population) by city.location").size()
        );
    }

    @SuppressWarnings("unchecked")
    private List<List<Object>> rowsOf(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        List<List<Object>> rows = (List<List<Object>>) response.get("datarows");
        assertNotNull("Response missing 'datarows' for query: " + ppl, rows);
        return rows;
    }

    /**
     * The shapeless-object and object-only cases at two shards. Both are covered at one shard in
     * {@link ObjectFieldIT}, where the reduce path is unreachable — and both produce a typed NULL or
     * a zero-column scan, the shape most likely to behave differently once fragments are reduced.
     */
    public void testShapelessAndObjectOnlyIndicesAtTwoShards() throws IOException {
        String shapeless = "shapeless_ms_it";
        makeTwoShardIndex(shapeless, "\"id\":{\"type\":\"keyword\"},\"attrs\":{\"type\":\"object\"}", false);
        bulkInto(shapeless, "{\"index\":{}}\n{\"id\":\"1\"}\n{\"index\":{}}\n{\"id\":\"2\"}\n");
        assertEquals(2, rowsOf("source=" + shapeless + " | fields id, attrs").size());
        for (List<Object> row : rowsOf("source=" + shapeless + " | fields attrs")) {
            assertNull("a shapeless object resolves to null on every shard, got: " + row, row.get(0));
        }

        String objectOnly = "object_only_ms_it";
        makeTwoShardIndex(objectOnly, "\"meta\":{\"type\":\"object\"}", true);
        bulkInto(objectOnly, "{\"index\":{}}\n{\"meta\":{\"x\":1}}\n{\"index\":{}}\n{\"meta\":{\"x\":2}}\n");
        List<List<Object>> counted = rowsOf("source=" + objectOnly + " | stats count()");
        assertEquals(1, counted.size());
        assertEquals("both documents counted across shards", 2, ((Number) counted.get(0).get(0)).intValue());
    }

    private void makeTwoShardIndex(String index, String propsJson, boolean dynamicFalse) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "/" + index));
        } catch (Exception ignored) {}
        Request create = new Request("PUT", "/" + index);
        create.setJsonEntity(
            "{\"settings\":{\"index.pluggable.dataformat.enabled\":true,"
                + "\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\","
                + "\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"number_of_shards\":2,\"number_of_replicas\":0},"
                + "\"mappings\":{"
                + (dynamicFalse ? "\"dynamic\":false," : "")
                + "\"properties\":{" + propsJson + "}}}"
        );
        client().performRequest(create);
    }

    private void bulkInto(String index, String ndjson) throws IOException {
        Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
        bulk.setJsonEntity(ndjson);
        bulk.setOptions(bulk.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson"));
        client().performRequest(bulk);
    }
}
