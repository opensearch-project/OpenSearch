/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.sort.NestedSortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

/**
 * Positional correlation across the parallel arrays of a ClickHouse-style {@code Nested} group.
 *
 * <p>OTel traces model events as parallel arrays — {@code Events.Timestamp}, {@code Events.Name},
 * {@code Events.Attributes} — where index {@code i} of each describes the same event. Each is stored
 * as its own Parquet {@code LIST} column with independent offsets, so nothing in the storage layer
 * couples them: a document whose arrays differ in length writes cleanly and reads back with values
 * from different events paired together.
 *
 * <p>ClickHouse prevents this by rejecting the insert ("elements of Nested type have different array
 * sizes"). Declaring the object {@code nested} with {@code correlated: true} applies the same rule at
 * parse time, turning a silently mispaired row into a rejected document.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class NestedGroupCorrelationIT extends AbstractCompositeEngineIT {

    /** Correlated: the group is declared {@code nested}, stored as parallel position-paired columns. */
    private static final String CORRELATED_MAPPING = "{\"properties\":{"
        + "\"TraceId\":{\"type\":\"keyword\"},"
        + "\"Events\":{\"type\":\"nested\",\"correlated\":true,\"properties\":{"
        + "  \"Name\":{\"type\":\"keyword\",\"multi_value\":true},"
        + "  \"Kind\":{\"type\":\"keyword\",\"multi_value\":true}"
        + "}}}}";

    /** The same fields as a plain object: no group, so nothing couples the columns. */
    private static final String PLAIN_MAPPING = "{\"properties\":{"
        + "\"TraceId\":{\"type\":\"keyword\"},"
        + "\"Events\":{\"properties\":{"
        + "  \"Name\":{\"type\":\"keyword\",\"multi_value\":true},"
        + "  \"Kind\":{\"type\":\"keyword\",\"multi_value\":true}"
        + "}}}}";

    private Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    private void createIndex(String name, String mapping) {
        assertTrue(client().admin().indices().prepareCreate(name).setSettings(settings()).setMapping(mapping).get().isAcknowledged());
        ensureGreen(name);
    }

    private Exception indexRagged(String index) {
        // 2 names but only 1 kind: name[1] has no kind of its own, so any positional pairing is a guess.
        return expectThrows(
            Exception.class,
            () -> client().prepareIndex(index)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .setSource("{\"TraceId\":\"t\",\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\"]}}", XContentType.JSON)
                .get()
        );
    }

    /** Equal lengths are the correlated case and must be accepted. */
    public void testEqualLengthsAccepted() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-ok", CORRELATED_MAPPING);

        client().prepareIndex("grp-ok")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"TraceId\":\"t\",\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\",\"k2\"]}}", XContentType.JSON)
            .get();

        assertEquals(1, client().admin().indices().prepareStats("grp-ok").get().getIndex("grp-ok").getTotal().getDocs().getCount());
    }

    /** Ragged arrays in a correlated group are rejected rather than stored mispaired. */
    public void testUnequalLengthsRejectedWhenCorrelated() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-strict", CORRELATED_MAPPING);

        Exception e = indexRagged("grp-strict");
        assertTrue(
            "expected a nested-group length error, got: " + e.getMessage(),
            e.getMessage().contains("correlated group") || e.getMessage().contains("Events")
        );
        assertEquals(
            "the rejected document must not be indexed",
            0,
            client().admin().indices().prepareStats("grp-strict").get().getIndex("grp-strict").getTotal().getDocs().getCount()
        );
    }

    /**
     * As a plain object the same document is accepted, so existing indices are unaffected. This is the
     * silently-mispaired state that [correlated: true] exists to opt out of.
     */
    public void testUnequalLengthsAcceptedWhenNotCorrelated() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-default", PLAIN_MAPPING);

        client().prepareIndex("grp-default")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"TraceId\":\"t\",\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\"]}}", XContentType.JSON)
            .get();

        assertEquals(
            1,
            client().admin().indices().prepareStats("grp-default").get().getIndex("grp-default").getTotal().getDocs().getCount()
        );
    }

    /**
     * A {@code nested} query over a correlated group fails loudly. There is no document per element for
     * its block join to match, so it would otherwise return nothing at all and read as "no such event"
     * rather than "this engine cannot scope a query to one element".
     */
    public void testNestedQueryOnCorrelatedGroupIsRejected() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-query", CORRELATED_MAPPING);

        client().prepareIndex("grp-query")
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"TraceId\":\"t\",\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\",\"k2\"]}}", XContentType.JSON)
            .get();

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("grp-query")
                .setQuery(QueryBuilders.nestedQuery("Events", QueryBuilders.termQuery("Events.Name", "a"), ScoreMode.None))
                .get()
        );
        assertTrue("expected a correlated-group explanation, got: " + e.getMessage(), e.getMessage().contains("correlated"));
    }

    /**
     * A {@code nested} aggregation buckets the per-element documents. A correlated group has none, so
     * its child filter matches nothing and every bucket would come back empty — a plausible-looking
     * zero rather than an unsupported operation.
     */
    public void testNestedAggregationOnCorrelatedGroupIsRejected() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-agg", CORRELATED_MAPPING);
        indexOneSpan("grp-agg");

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("grp-agg").addAggregation(AggregationBuilders.nested("ev", "Events")).get()
        );
        assertTrue("expected a correlated-group explanation, got: " + e.getMessage(), e.getMessage().contains("correlated"));
    }

    /**
     * Nested sorting picks a value from the matching child documents. With none, the sort would
     * silently fall back to the missing value for every hit instead of reporting that it cannot work.
     */
    public void testNestedSortOnCorrelatedGroupIsRejected() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(1);
        createIndex("grp-sort", CORRELATED_MAPPING);
        indexOneSpan("grp-sort");

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareSearch("grp-sort")
                .addSort(SortBuilders.fieldSort("Events.Name").setNestedSort(new NestedSortBuilder("Events")))
                .get()
        );
        assertTrue("expected a correlated-group explanation, got: " + e.getMessage(), e.getMessage().contains("correlated"));
    }

    private void indexOneSpan(String index) {
        client().prepareIndex(index)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .setSource("{\"TraceId\":\"t\",\"Events\":{\"Name\":[\"a\",\"b\"],\"Kind\":[\"k1\",\"k2\"]}}", XContentType.JSON)
            .get();
    }
}
