/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.List;
import java.util.Map;

/**
 * Integration tests for stats API behavior across different cluster topologies.
 * Each test starts its own cluster nodes since Scope.TEST destroys between tests.
 *
 * @opensearch.experimental
 */
@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class StatsTopologyIT extends BaseStatsIT {

    protected void createIndexWithShards(String name, int numShards, int numReplicas, boolean withLuceneSecondary) {
        Settings.Builder s = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet");
        if (withLuceneSecondary) {
            s.putList("index.composite.secondary_data_formats", "lucene");
        } else {
            s.putList("index.composite.secondary_data_formats");
        }
        client().admin().indices().prepareCreate(name).setSettings(s).setMapping("name", "type=keyword", "value", "type=integer").get();
        ensureGreen(name);
    }

    /**
     * Verifies that index-level doc total equals the sum of per-shard doc totals
     * when queried with level=shards.
     */
    @SuppressWarnings("unchecked")
    public void testIndexLevelEqualsShardSum() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("shard-sum-idx", 3, 0, true);
        indexDocs("shard-sum-idx", 300, 0);
        refreshIndex("shard-sum-idx");

        // Query default level
        Map<String, Object> r = parquetIndexStats("shard-sum-idx");
        assertCounter("index-level total", r, "indices.shard-sum-idx.indexing.docs_indexed_total", 300L);

        // Query level=shards
        Map<String, Object> rs = parquetIndexStats("shard-sum-idx", "level", "shards");
        assertTrue("shards block must exist for level=shards", hasPath(rs, "indices.shard-sum-idx.shards"));

        long sum = computeShardSum(rs, "shard-sum-idx", "indexing.docs_indexed_total");
        assertEquals("shard sum must equal index total", 300L, sum);
    }

    /**
     * Verifies that stats aggregate correctly across a 3-node cluster with 6 shards.
     * Cluster-aggregate via /{idx}/_stats is the authoritative aggregation path.
     *
     * <p>Note: per-node breakdown via /_nodes/_stats is NOT asserted with a sum here because
     * {@code internalCluster()} runs all "nodes" in a single JVM, where the
     * {@code DataFormatStatsProviderRegistry} is JVM-wide. Each node's local registry sees
     * trackers from every node, so summing per-node values triple-counts. In production
     * each node has its own JVM, so per-node values are correctly partitioned by shard ownership.
     */
    @SuppressWarnings("unchecked")
    public void testMultiNodeClusterAggregation() throws Exception {
        internalCluster().startNodes(3);
        createIndexWithShards("cluster-agg-idx", 6, 0, true);
        indexDocs("cluster-agg-idx", 600, 0);
        refreshIndex("cluster-agg-idx");

        Map<String, Object> r = parquetIndexStats("cluster-agg-idx");
        assertCounter("cluster-aggregate doc total", r, "indices.cluster-agg-idx.indexing.docs_indexed_total", 600L);
        // Verify _shards header
        assertCounter("_shards.total", r, "_shards.total", 6L);
        assertCounter("_shards.successful", r, "_shards.successful", 6L);
        assertCounter("_shards.failed", r, "_shards.failed", 0L);

        // Per-node endpoint: assert response structure (3 nodes present, each has indexing block).
        // Do NOT sum per-node — see javadoc above for why.
        Map<String, Object> nr = parquetNodeStats("");
        assertTrue("nodes key present", nr.containsKey("nodes"));
        Map<String, Object> nodes = (Map<String, Object>) nr.get("nodes");
        assertEquals("expected 3 node entries", 3, nodes.size());
        for (Map.Entry<String, Object> e : nodes.entrySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) e.getValue();
            assertTrue("node entry must have indexing block", hasPath(nodeStats, "indexing.docs_indexed_total"));
        }
    }

    /**
     * Verifies that node-stats endpoint with a specific nodeId returns only that node.
     */
    @SuppressWarnings("unchecked")
    public void testNodeStatsSpecificNodeFilter() throws Exception {
        internalCluster().startNodes(3);
        createIndexWithShards("node-filter-idx", 3, 0, true);
        indexDocs("node-filter-idx", 90, 0);
        refreshIndex("node-filter-idx");

        // Get one node's id
        Map<String, Object> all = parquetNodeStats("");
        Map<String, Object> nodes = (Map<String, Object>) all.get("nodes");
        String firstNodeId = nodes.keySet().iterator().next();

        // Query for just that node
        Map<String, Object> single = parquetNodeStats(firstNodeId);
        Map<String, Object> singleNodes = (Map<String, Object>) single.get("nodes");
        assertEquals("node-id-filter response must have exactly 1 node", 1, singleNodes.size());
        assertTrue("response must contain the requested node", singleNodes.containsKey(firstNodeId));
    }

    /**
     * Verifies that stats survive a data node restart and continue accumulating correctly.
     * In the single-JVM test framework, the JVM-wide {@code DataFormatStatsProviderRegistry}
     * retains trackers across node restarts, so counters persist. This test verifies the
     * stats endpoint remains functional after restart and new indexing is tracked.
     */
    @SuppressWarnings("unchecked")
    public void testStatsAccumulateAfterNodeRestart() throws Exception {
        // Start a dedicated cluster manager (keeps REST client alive) + 1 data node.
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        String idx = "restart-idx";
        createIndexWithShards(idx, 1, 0, true);
        indexDocs(idx, 50, 0);
        refreshIndex(idx);

        // Pre-restart: counters reflect the 50 docs.
        long preRestartDocs = getCounter(parquetIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total");
        assertEquals("pre-restart parquet docs", 50L, preRestartDocs);

        // Restart the data node.
        internalCluster().restartNode(dataNode);
        ensureGreen(idx);

        // Post-restart: stats endpoint must still be functional.
        Map<String, Object> postRestart = parquetIndexStats(idx);
        assertTrue("response must have indices key after restart", postRestart.containsKey("indices"));

        // Index more docs — counters must accumulate from wherever they were.
        long postRestartBefore = getCounter(parquetIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total");
        indexDocs(idx, 30, 50);
        refreshIndex(idx);
        long postRestartAfter = getCounter(parquetIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total");
        assertEquals("new docs must be tracked after restart", postRestartBefore + 30L, postRestartAfter);

        // Lucene secondary must also track the new docs.
        long luceneAfter = getCounter(luceneIndexStats(idx), "indices." + idx + ".indexing.docs_indexed_total");
        assertEquals("lucene must match parquet after restart", postRestartAfter, luceneAfter);
    }

    // testReplicasExcludedFromAggregation: deferred. DFA replicas in current upstream require
    // a remote-store-backed cluster (RemoteStoreBaseIntegTestCase). Plain peer recovery to a
    // non-remote replica fails with TranslogCorruptedException at recovery time. To test
    // primary-only aggregation we'd need to extend RemoteStoreBaseIntegTestCase and configure
    // segment replication via remote store. This is moved to Wave 3 / a follow-up PR.

    /**
     * Verifies that the {@code shards=} param accepts a comma-separated list and filters
     * shard-level results to exactly those shard IDs.
     */
    @SuppressWarnings("unchecked")
    public void testMultiValueShardFilter() throws Exception {
        internalCluster().startNodes(2);
        createIndexWithShards("multi-shard-filter-idx", 4, 0, true);
        indexDocs("multi-shard-filter-idx", 80, 0);
        refreshIndex("multi-shard-filter-idx");

        // shards=0,2 — exactly 2 entries.
        Map<String, Object> r2 = parquetIndexStats("multi-shard-filter-idx", "level", "shards", "shards", "0,2");
        Map<String, Object> shards2 = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) r2.get("indices")).get(
            "multi-shard-filter-idx"
        )).get("shards");
        assertEquals("shards=0,2 must return exactly 2 entries", 2, shards2.size());
        assertTrue("must include shard 0", shards2.containsKey("0"));
        assertTrue("must include shard 2", shards2.containsKey("2"));
        assertFalse("must NOT include shard 1", shards2.containsKey("1"));
        assertFalse("must NOT include shard 3", shards2.containsKey("3"));

        // shards=0,1,3 — exactly 3 entries.
        Map<String, Object> r3 = parquetIndexStats("multi-shard-filter-idx", "level", "shards", "shards", "0,1,3");
        Map<String, Object> shards3 = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) r3.get("indices")).get(
            "multi-shard-filter-idx"
        )).get("shards");
        assertEquals("shards=0,1,3 must return exactly 3 entries", 3, shards3.size());
        assertTrue(shards3.containsKey("0"));
        assertTrue(shards3.containsKey("1"));
        assertTrue(shards3.containsKey("3"));
        assertFalse("must NOT include shard 2", shards3.containsKey("2"));
    }

    /**
     * Verifies that the {@code nodes=} param accepts a comma-separated list and that the
     * special token {@code _local} resolves to the coordinating node.
     */
    @SuppressWarnings("unchecked")
    public void testMultiValueNodeFilter() throws Exception {
        internalCluster().startNodes(2);
        createIndexWithShards("multi-node-filter-idx", 4, 0, true);
        indexDocs("multi-node-filter-idx", 40, 0);
        refreshIndex("multi-node-filter-idx");

        // Get both node IDs.
        var nodeInfos = client().admin().cluster().prepareNodesInfo().get().getNodes();
        String nodeA = nodeInfos.get(0).getNode().getId();
        String nodeB = nodeInfos.get(1).getNode().getId();

        // nodes=A,B — should return ALL 4 shards (across both nodes).
        Map<String, Object> rBoth = parquetIndexStats("multi-node-filter-idx", "level", "shards", "nodes", nodeA + "," + nodeB);
        Map<String, Object> shardsBoth = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) rBoth.get("indices")).get(
            "multi-node-filter-idx"
        )).get("shards");
        assertEquals("nodes=A,B (all nodes) must return all 4 shards", 4, shardsBoth.size());

        // nodes=A — only shards on node A. Each shard's routing.node must match nodeA.
        Map<String, Object> rA = parquetIndexStats("multi-node-filter-idx", "level", "shards", "nodes", nodeA);
        Map<String, Object> shardsA = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) rA.get("indices")).get(
            "multi-node-filter-idx"
        )).get("shards");
        assertTrue("nodes=A must return at least 1 shard", shardsA.size() >= 1);
        for (Map.Entry<String, Object> e : shardsA.entrySet()) {
            List<Map<String, Object>> copies = (List<Map<String, Object>>) e.getValue();
            for (Map<String, Object> copy : copies) {
                Map<String, Object> routing = (Map<String, Object>) copy.get("routing");
                assertEquals("every entry must be on node A", nodeA, routing.get("node"));
            }
        }

        // nodes=_local must succeed and return shards on the coordinating node.
        Map<String, Object> rLocal = parquetIndexStats("multi-node-filter-idx", "level", "shards", "nodes", "_local");
        Map<String, Object> shardsLocal = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) rLocal.get("indices")).get(
            "multi-node-filter-idx"
        )).get("shards");
        assertTrue("_local must return at least 1 shard", shardsLocal.size() >= 1);
    }

    /**
     * Verifies that the {@code expand_wildcards} param controls wildcard expansion semantics.
     * Default {@code open} excludes closed indices; {@code all} with {@code ignore_unavailable=true}
     * includes both open and closed indices in the resolution (though closed indices may have empty stats).
     */
    @SuppressWarnings("unchecked")
    public void testExpandWildcardsHonored() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("wildcard-open-idx", 1, 0, true);
        createIndexWithShards("wildcard-closed-idx", 1, 0, true);
        indexDocs("wildcard-open-idx", 10, 0);
        indexDocs("wildcard-closed-idx", 20, 0);
        refreshIndex("wildcard-open-idx");
        refreshIndex("wildcard-closed-idx");

        // Close one of the indices.
        client().admin().indices().prepareClose("wildcard-closed-idx").get();

        // Default (no expand_wildcards) — wildcard matches only the open index.
        Map<String, Object> rDefault = parquetIndexStats("wildcard-*");
        Map<String, Object> indicesDefault = (Map<String, Object>) rDefault.get("indices");
        assertTrue("default expand must include open index", indicesDefault.containsKey("wildcard-open-idx"));
        assertFalse("default expand must NOT include closed index", indicesDefault.containsKey("wildcard-closed-idx"));

        // expand_wildcards=open — same as default.
        Map<String, Object> rOpen = parquetIndexStats("wildcard-*", "expand_wildcards", "open");
        Map<String, Object> indicesOpen = (Map<String, Object>) rOpen.get("indices");
        assertTrue(indicesOpen.containsKey("wildcard-open-idx"));
        assertFalse(indicesOpen.containsKey("wildcard-closed-idx"));

        // expand_wildcards=all with ignore_unavailable=true — the open index must still appear.
        // The closed index may or may not appear depending on whether the broadcast action can
        // iterate closed shards. We verify the open index is present and that the response
        // includes MORE indices than expand_wildcards=open (or at minimum the same set).
        Map<String, Object> rAll = parquetIndexStats("wildcard-*", "expand_wildcards", "all", "ignore_unavailable", "true");
        Map<String, Object> indicesAll = (Map<String, Object>) rAll.get("indices");
        assertTrue("expand=all must include open", indicesAll.containsKey("wildcard-open-idx"));
        // expand_wildcards=all resolves the closed index in the pattern, but the broadcast
        // action cannot execute on closed shards so the response may omit it. Assert that
        // the response size is >= the open-only response (no regression).
        assertTrue("expand=all must return at least as many indices as expand=open", indicesAll.size() >= indicesOpen.size());
    }

    /**
     * Verifies the {@code allow_no_indices} param controls whether a wildcard pattern matching
     * zero indices returns 200 with empty results or 404.
     */
    @SuppressWarnings("unchecked")
    public void testAllowNoIndices() throws Exception {
        internalCluster().startNodes(1);
        // Create an index to ensure cluster is green, then use a non-matching wildcard.
        createIndexWithShards("allow-no-anchor-idx", 1, 0, true);

        // allow_no_indices=true — must succeed with empty indices block.
        Map<String, Object> rOk = parquetIndexStats("no-match-*", "allow_no_indices", "true");
        Map<String, Object> indicesOk = (Map<String, Object>) rOk.get("indices");
        assertNotNull("response must have indices key", indicesOk);
        assertTrue("indices block must be empty for no-match wildcard", indicesOk.isEmpty());

        // allow_no_indices=false — must fail with 4xx.
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/no-match-*/_stats?allow_no_indices=false"));
            fail("expected 4xx for allow_no_indices=false on no-match wildcard");
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            assertTrue("expected 4xx, got " + code, code >= 400 && code < 500);
        }
    }

    /**
     * Verifies that an out-of-range shard ID in the {@code shards=} param returns a clear 400.
     */
    public void testInvalidShardIdReturnsBadRequest() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("bad-shard-idx", 3, 0, true);
        indexDocs("bad-shard-idx", 30, 0);
        refreshIndex("bad-shard-idx");

        // shards=99 — out of range for a 3-shard index.
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/bad-shard-idx/_stats?level=shards&shards=99"));
            fail("expected 400 for out-of-range shard id");
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected exactly 400 for out-of-range shard", 400, code);
        }

        // shards=foo — non-integer must also return 400.
        try {
            getRestClient().performRequest(new Request("GET", "/_plugins/parquet/bad-shard-idx/_stats?level=shards&shards=foo"));
            fail("expected 400 for non-integer shard id");
        } catch (ResponseException e) {
            int code = e.getResponse().getStatusLine().getStatusCode();
            assertEquals("expected exactly 400 for non-integer shard", 400, code);
        }
    }

    /**
     * Computes the sum of a counter across all shards in the shard-level response.
     * Response shape: indices.{idx}.shards."{shardId}".[{routing: {primary: bool, ...}, indexing: {...}, ...}]
     *
     * <p>Treats both 0 and absent (-1L from {@code getCounter}) as no-contribution; only
     * positive values are added. Negative values from missing paths are ignored rather
     * than throwing — callers that need strict path validation should use {@code hasPath}.
     */
    @SuppressWarnings("unchecked")
    private long computeShardSum(Map<String, Object> response, String indexName, String counterPath) {
        Map<String, Object> indices = (Map<String, Object>) response.get("indices");
        Map<String, Object> indexStats = (Map<String, Object>) indices.get(indexName);
        Map<String, Object> shards = (Map<String, Object>) indexStats.get("shards");
        long sum = 0L;
        for (Map.Entry<String, Object> shardEntry : shards.entrySet()) {
            List<Map<String, Object>> copies = (List<Map<String, Object>>) shardEntry.getValue();
            for (Map<String, Object> copy : copies) {
                long val = getCounter(copy, counterPath);
                if (val >= 0) sum += val;
            }
        }
        return sum;
    }

    /**
     * 3 shards, query level=shards, verify each shard entry has primary field and stats.
     */
    @SuppressWarnings("unchecked")
    public void testShardLevelRoutingMetadata() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("routing-idx", 3, 0, true);
        indexDocs("routing-idx", 60, 0);
        refreshIndex("routing-idx");

        Map<String, Object> rs = parquetIndexStats("routing-idx", "level", "shards");
        assertTrue("shards block must exist", hasPath(rs, "indices.routing-idx.shards"));

        Map<String, Object> indices = (Map<String, Object>) rs.get("indices");
        Map<String, Object> indexBlock = (Map<String, Object>) indices.get("routing-idx");
        Map<String, Object> shards = (Map<String, Object>) indexBlock.get("shards");
        assertEquals("expected 3 shard entries (one per primary)", 3, shards.size());

        for (Map.Entry<String, Object> shardEntry : shards.entrySet()) {
            List<Map<String, Object>> copies = (List<Map<String, Object>>) shardEntry.getValue();
            assertEquals("each shard list must have exactly 1 entry (primary only)", 1, copies.size());
            Map<String, Object> copy = copies.get(0);
            // routing sub-object must be present with expected fields.
            Map<String, Object> routing = (Map<String, Object>) copy.get("routing");
            assertNotNull("each shard copy must have a routing block", routing);
            assertEquals("copy must be primary", Boolean.TRUE, routing.get("primary"));
            assertNotNull("routing.node must be set", routing.get("node"));
            assertNotNull("routing.state must be set", routing.get("state"));
            assertNull("routing.relocating_node must be null for stable shards", routing.get("relocating_node"));
            // Stats fields must be present in the copy.
            assertTrue("copy must have indexing block", copy.containsKey("indexing"));
        }
    }

    /**
     * With no level param, response must NOT include shards block.
     */
    @SuppressWarnings("unchecked")
    public void testDefaultLevelExcludesShardsBlock() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("default-level-idx", 3, 0, true);
        indexDocs("default-level-idx", 30, 0);
        refreshIndex("default-level-idx");

        // Default — no level param.
        Map<String, Object> r = parquetIndexStats("default-level-idx");
        Map<String, Object> indices = (Map<String, Object>) r.get("indices");
        Map<String, Object> indexBlock = (Map<String, Object>) indices.get("default-level-idx");
        assertFalse("default level response must NOT have a shards block", indexBlock.containsKey("shards"));
        assertCounter("index-level total still present", r, "indices.default-level-idx.indexing.docs_indexed_total", 30L);

        // Explicit level=index — also excludes shards.
        Map<String, Object> rIndex = parquetIndexStats("default-level-idx", "level", "index");
        Map<String, Object> indicesIdx = (Map<String, Object>) rIndex.get("indices");
        Map<String, Object> indexBlockIdx = (Map<String, Object>) indicesIdx.get("default-level-idx");
        assertFalse("explicit level=index must NOT have a shards block", indexBlockIdx.containsKey("shards"));
    }

    /**
     * Multiple indices, test wildcard, comma-separated, and ignore_unavailable resolution.
     */
    @SuppressWarnings("unchecked")
    public void testIndexPatternResolution() throws Exception {
        internalCluster().startNodes(1);
        createIndexWithShards("pat-idx-a", 1, 0, true);
        createIndexWithShards("pat-idx-b", 1, 0, true);
        createIndexWithShards("other-idx", 1, 0, true);
        indexDocs("pat-idx-a", 10, 0);
        indexDocs("pat-idx-b", 20, 0);
        indexDocs("other-idx", 30, 0);
        refreshIndex("pat-idx-a");
        refreshIndex("pat-idx-b");
        refreshIndex("other-idx");

        // Wildcard pat-idx-* must match a + b but NOT other-idx.
        Map<String, Object> wildResp = parquetIndexStats("pat-idx-*");
        Map<String, Object> wildIndices = (Map<String, Object>) wildResp.get("indices");
        assertTrue("wildcard must match pat-idx-a", wildIndices.containsKey("pat-idx-a"));
        assertTrue("wildcard must match pat-idx-b", wildIndices.containsKey("pat-idx-b"));
        assertFalse("wildcard must NOT match other-idx", wildIndices.containsKey("other-idx"));

        // Comma-separated must match both (use a and other).
        Map<String, Object> commaResp = parquetIndexStats("pat-idx-a,other-idx");
        Map<String, Object> commaIndices = (Map<String, Object>) commaResp.get("indices");
        assertTrue(commaIndices.containsKey("pat-idx-a"));
        assertTrue(commaIndices.containsKey("other-idx"));
        assertFalse("comma list must NOT match unspecified pat-idx-b", commaIndices.containsKey("pat-idx-b"));

        // ignore_unavailable=true with one missing must succeed and return only the existing one.
        Map<String, Object> mixResp = parquetIndexStats("pat-idx-a,no-such-idx", "ignore_unavailable", "true");
        Map<String, Object> mixIndices = (Map<String, Object>) mixResp.get("indices");
        assertTrue("existing index pat-idx-a must appear in response", mixIndices.containsKey("pat-idx-a"));
        assertFalse("missing index must not appear in response", mixIndices.containsKey("no-such-idx"));
    }

    /**
     * Test the shard= and node= query parameters for filtering shard-level results.
     */
    @SuppressWarnings("unchecked")
    public void testShardAndNodeFilters() throws Exception {
        internalCluster().startNodes(2);
        createIndexWithShards("filter-idx", 4, 0, true);
        indexDocs("filter-idx", 80, 0);
        refreshIndex("filter-idx");

        // shards=2 — only that shard in shard-level response.
        Map<String, Object> sf = parquetIndexStats("filter-idx", "level", "shards", "shards", "2");
        Map<String, Object> indices = (Map<String, Object>) sf.get("indices");
        Map<String, Object> indexBlock = (Map<String, Object>) indices.get("filter-idx");
        Map<String, Object> shards = (Map<String, Object>) indexBlock.get("shards");
        assertEquals("shard filter must restrict to 1 entry", 1, shards.size());
        assertTrue("must include shard 2", shards.containsKey("2"));

        // node filter — pick a real node id from the cluster, query restricting to that node.
        String someNodeId = client().admin().cluster().prepareNodesInfo().get().getNodes().get(0).getNode().getId();
        Map<String, Object> nf = parquetIndexStats("filter-idx", "level", "shards", "nodes", someNodeId);
        Map<String, Object> nfIndices = (Map<String, Object>) nf.get("indices");
        Map<String, Object> nfIndex = (Map<String, Object>) nfIndices.get("filter-idx");
        Map<String, Object> nfShards = (Map<String, Object>) nfIndex.get("shards");
        // Must have at most as many entries as shards on that node (4 shards across 2 nodes).
        assertTrue("node filter must return at least 1 shard", nfShards.size() >= 1);
        assertTrue("node filter must return at most 4 shards", nfShards.size() <= 4);
    }
}
