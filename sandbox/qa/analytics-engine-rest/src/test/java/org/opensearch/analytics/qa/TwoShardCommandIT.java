/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 2-shard correctness for the PPL command surface ({@code cmd/}). Verified at 2 shards: rex, table,
 * spath, rename, fillnull, bin, top, chart, lookup. The rest are muted (see {@link #knownIssues()}).
 */
public class TwoShardCommandIT extends TwoShardReduceTestCase {

    @Override
    protected Map<String, Boolean> tiers() {
        return Map.of("cmd", false);
    }

    @Override
    protected Map<String, String> knownIssues() {
        // append/multisearch/regex/appendpipe previously failed: these union/multi-input shapes had an
        // arm that resolved to lucene while the union stayed datafusion, and PlanForker couldn't
        // reconcile the per-arm backends (the union got zero plan alternatives). Fixed — they now pass
        // at 2 shards. The rest stay muted for unrelated reasons:
        Map<String, String> m = new LinkedHashMap<>();
        m.put("cmd_search", "search numeric comparison lowers to a Lucene query_string that matches zero docs on numeric fields (wrong result)");
        m.put("cmd_appendcols", "not in the PPL grammar (SyntaxCheckException)");
        return m;
    }

    /** Override solely to attach @AwaitsFix; inherited body is unchanged. */
    // Pending sql cluster A+D: combined UDT bridging + value rendering
    @Override
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/sql/pull/<TBD>")
    public void testReduceCorrectnessAcrossTwoShards() throws IOException {
        super.testReduceCorrectnessAcrossTwoShards();
    }
}
