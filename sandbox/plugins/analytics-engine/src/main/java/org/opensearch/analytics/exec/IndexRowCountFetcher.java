/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.transport.client.Client;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToLongFunction;

/**
 * Per-query primary-shard doc-count fetcher feeding {@link PlannerContext#getTableRowCounts}.
 *
 * <p>Walks the input {@link RelNode} collecting every {@link TableScan}'s qualified table name,
 * issues a single {@code IndicesStats} call scoped to those indices ({@code primaries.docs.count}
 * only, no shard-level breakdown), and returns a {@link ToLongFunction} mapping index name → row
 * count. Indices missing from the response (or fetch failure entirely) yield
 * {@link PlannerContext#UNKNOWN_ROW_COUNT}, which {@code OpenSearchTableScanRule}'s
 * {@code IndexNameTable} treats as "fall back to Calcite default" so plans still work.
 *
 * <p>The fetch is synchronous and runs on the caller's thread (DefaultPlanExecutor's SEARCH
 * executor task) before planning starts. IndicesStats is a hot cluster API; on a healthy
 * cluster the round-trip is tens of milliseconds. The cost-aware planner relies on these
 * counts to discriminate broadcast (cost grows with build rows × probe nodes) from
 * coordinator-centric (cost grows with both sides' rows + 10/ER). Without them the cost model
 * collapses to defaults (Calcite's 100 rows/scan) and broadcast loses the cost race against
 * the cheaper-on-paper SINGLETON gather.
 *
 * @opensearch.internal
 */
public final class IndexRowCountFetcher {

    private static final Logger LOGGER = LogManager.getLogger(IndexRowCountFetcher.class);

    private IndexRowCountFetcher() {}

    /**
     * Fetches primary-shard doc counts for every index referenced by a {@link TableScan} in
     * {@code root}'s tree. Returns a lookup; missing indices map to
     * {@link PlannerContext#UNKNOWN_ROW_COUNT}.
     *
     * @param root the raw RelNode about to be planned (pre-CBO).
     * @param client used for the IndicesStats admin call. May be null in tests; non-null in
     *     production via {@code DefaultPlanExecutor}'s injected NodeClient.
     */
    public static ToLongFunction<String> fetchFor(RelNode root, Client client) {
        Set<String> indexNames = new HashSet<>();
        collectTableScans(root, indexNames);
        if (indexNames.isEmpty() || client == null) {
            return PlannerContext.DEFAULT_TABLE_ROW_COUNTS;
        }
        Map<String, Long> rowCounts = fetchPrimaryDocCounts(client, indexNames);
        return name -> rowCounts.getOrDefault(name, PlannerContext.UNKNOWN_ROW_COUNT);
    }

    private static void collectTableScans(RelNode node, Set<String> out) {
        if (node instanceof TableScan scan) {
            List<String> qualified = scan.getTable().getQualifiedName();
            if (!qualified.isEmpty()) {
                out.add(qualified.get(qualified.size() - 1));
            }
        }
        for (RelNode input : node.getInputs()) {
            collectTableScans(input, out);
        }
    }

    private static Map<String, Long> fetchPrimaryDocCounts(Client client, Set<String> indexNames) {
        try {
            IndicesStatsRequest request = new IndicesStatsRequest();
            request.indices(indexNames.toArray(new String[0]));
            request.clear();
            request.docs(true);
            IndicesStatsResponse response = client.admin().indices().stats(request).actionGet();
            Map<String, Long> rowCounts = new HashMap<>();
            for (Map.Entry<String, IndexStats> entry : response.getIndices().entrySet()) {
                CommonStats primaries = entry.getValue().getPrimaries();
                DocsStats docs = primaries == null ? null : primaries.getDocs();
                if (docs != null) {
                    rowCounts.put(entry.getKey(), docs.getCount());
                }
            }
            return rowCounts;
        } catch (Exception e) {
            // IndicesStats can fail for many transient reasons (cluster red, an index closed
            // mid-flight, etc.). Don't fail the query — fall back to UNKNOWN row counts and
            // let CBO use Calcite's default. The query still runs; it just may pick a less
            // optimal join strategy until stats are available again.
            LOGGER.warn(new ParameterizedMessage("IndicesStats fetch failed for {}; row counts will be unknown", indexNames), e);
            return Map.of();
        }
    }
}
