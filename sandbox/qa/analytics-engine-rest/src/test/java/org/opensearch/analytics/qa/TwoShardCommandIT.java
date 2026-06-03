/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

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
        // The search/append/regex/multisearch commands carry a residual filter that routes through the
        // DataFusion indexed executor, which at 2 shards SIGSEGVs the data node on the Arrow view C-data
        // export (upcallLinker.cpp:137) — so they MUST be skipped, not run (a crash breaks other tests).
        String crash = "residual filter -> indexed-executor SIGSEGV at 2 shards (crashes the data node)";
        Map<String, String> m = new LinkedHashMap<>();
        m.put("cmd_search", crash);
        m.put("cmd_append", crash);
        m.put("cmd_regex", crash);
        m.put("cmd_multisearch", crash);
        m.put("cmd_appendcols", "not in the PPL grammar (SyntaxCheckException)");
        m.put("cmd_timechart", "requires an @timestamp field");
        m.put("cmd_appendpipe", "lowers to OpenSearchUnion with a lucene branch vs datafusion main (#21867)");
        return m;
    }
}
