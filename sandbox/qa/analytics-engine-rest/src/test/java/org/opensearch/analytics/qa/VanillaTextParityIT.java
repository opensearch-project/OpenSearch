/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.junit.AfterClass;
import org.opensearch.client.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Text / keyword predicate results must equal vanilla OpenSearch's.
 *
 * <p>Expected counts were recorded from a vanilla 3.9 node (core + job-scheduler + opensearch-sql,
 * no analytics engine) holding the same mapping and documents, via {@code _plugins/_ppl}. Vanilla
 * pushes text predicates down in two different ways — whole-value ({@code =, !=, IN, NOT IN,
 * LIKE}, through the keyword multifield when present) and token ranges on the analyzed field
 * ({@code >, <, BETWEEN}) — and every execution mode here must reproduce both:
 * Lucene driving the stage, DataFusion driving with performance delegation, and DataFusion with
 * Lucene predicates blocked.
 *
 * <p>Fields: {@code t} text; {@code tk} text + keyword; {@code tn} text + normalized keyword;
 * {@code ti} text + keyword with {@code ignore_above: 10}; {@code k} keyword; {@code kn}
 * normalized keyword. Every 7th document has none of them (nulls).
 */
public class VanillaTextParityIT extends AnalyticsRestTestCase {

    private static final String INDEX = "vanilla_text_parity";
    private static final String[] VALUES = { "Apple pie", "apple Pie", "banana split", "Cherry tart", "zebra crossing", "Mango", null };
    private static final String[] FIELDS = { "t", "tk", "tn", "ti", "k", "kn" };
    private static final String BLOCK_ALL =
        "[\"EQUALS\",\"NOT_EQUALS\",\"LIKE\",\"GREATER_THAN\",\"LESS_THAN\",\"GREATER_THAN_OR_EQUAL\","
            + "\"LESS_THAN_OR_EQUAL\",\"SARG_PREDICATE\",\"IS_NULL\",\"IS_NOT_NULL\"]";

    /** Vanilla counts for {@code where <predicate> | stats count()}. */
    private static final Map<String, Long> VANILLA = Map.ofEntries(
        Map.entry("t > 'b'", 60L),
        Map.entry("tk > 'b'", 60L),
        Map.entry("tn > 'b'", 60L),
        Map.entry("ti > 'b'", 60L),
        Map.entry("k > 'b'", 20L),
        Map.entry("kn > 'b'", 40L),
        Map.entry("t >= 'apple' and t < 'b'", 20L),
        Map.entry("tk >= 'apple' and tk < 'b'", 20L),
        Map.entry("tn >= 'apple' and tn < 'b'", 20L),
        Map.entry("ti >= 'apple' and ti < 'b'", 20L),
        Map.entry("k >= 'apple' and k < 'b'", 10L),
        Map.entry("kn >= 'apple' and kn < 'b'", 20L),
        Map.entry("t = 'Apple pie'", 10L),
        Map.entry("tk = 'Apple pie'", 10L),
        Map.entry("tn = 'Apple pie'", 20L),
        Map.entry("ti = 'Apple pie'", 10L),
        Map.entry("k = 'Apple pie'", 10L),
        Map.entry("kn = 'Apple pie'", 20L),
        Map.entry("t = 'apple'", 0L),
        Map.entry("tk = 'apple'", 0L),
        Map.entry("tn = 'apple'", 0L),
        Map.entry("ti = 'apple'", 0L),
        Map.entry("k = 'apple'", 0L),
        Map.entry("kn = 'apple'", 0L),
        Map.entry("t != 'Apple pie'", 50L),
        Map.entry("tk != 'Apple pie'", 50L),
        Map.entry("tn != 'Apple pie'", 40L),
        Map.entry("ti != 'Apple pie'", 50L),
        Map.entry("k != 'Apple pie'", 50L),
        Map.entry("kn != 'Apple pie'", 40L),
        Map.entry("t in ('Apple pie', 'Mango')", 20L),
        Map.entry("tk in ('Apple pie', 'Mango')", 20L),
        Map.entry("tn in ('Apple pie', 'Mango')", 30L),
        Map.entry("ti in ('Apple pie', 'Mango')", 20L),
        Map.entry("k in ('Apple pie', 'Mango')", 20L),
        Map.entry("kn in ('Apple pie', 'Mango')", 30L),
        Map.entry("not t in ('Apple pie', 'Mango')", 40L),
        Map.entry("not tk in ('Apple pie', 'Mango')", 40L),
        Map.entry("not tn in ('Apple pie', 'Mango')", 30L),
        Map.entry("not ti in ('Apple pie', 'Mango')", 40L),
        Map.entry("not k in ('Apple pie', 'Mango')", 40L),
        Map.entry("not kn in ('Apple pie', 'Mango')", 30L),
        Map.entry("t like 'apple p%'", 20L),
        Map.entry("tk like 'apple p%'", 20L),
        Map.entry("tn like 'apple p%'", 20L),
        Map.entry("ti like 'apple p%'", 20L),
        Map.entry("k like 'apple p%'", 20L),
        Map.entry("kn like 'apple p%'", 20L),
        Map.entry("isnull(t)", 10L),
        Map.entry("isnull(tk)", 10L),
        Map.entry("isnull(tn)", 10L),
        Map.entry("isnull(ti)", 10L),
        Map.entry("isnull(k)", 10L),
        Map.entry("isnull(kn)", 10L),
        Map.entry("isnotnull(t)", 60L),
        Map.entry("isnotnull(tk)", 60L),
        Map.entry("isnotnull(tn)", 60L),
        Map.entry("isnotnull(ti)", 60L),
        Map.entry("isnotnull(k)", 60L),
        Map.entry("isnotnull(kn)", 60L),
        Map.entry("t = 'zebra crossing'", 10L),
        Map.entry("tk = 'zebra crossing'", 10L),
        Map.entry("tn = 'zebra crossing'", 10L),
        Map.entry("ti = 'zebra crossing'", 0L),
        Map.entry("k = 'zebra crossing'", 10L),
        Map.entry("kn = 'zebra crossing'", 10L)
    );

    /**
     * Known, accepted difference: with Lucene predicates blocked, DataFusion compares the parquet
     * value, which keeps values longer than the keyword multifield's {@code ignore_above}; vanilla's
     * term query cannot see them (see {@code FieldStorageResolver}). Results are guaranteed only
     * for values within the limit.
     */
    private static final Map<String, Long> LUCENE_BLOCKED_OVERRIDES = Map.of("ti = 'zebra crossing'", 10L);

    private static boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned) {
            return;
        }
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {
            // index may not exist
        }
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity(
            "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
                + "\"index.pluggable.dataformat.enabled\":true,\"index.pluggable.dataformat\":\"composite\","
                + "\"index.composite.primary_data_format\":\"parquet\",\"index.composite.secondary_data_formats\":[\"lucene\"],"
                + "\"analysis\":{\"normalizer\":{\"lc\":{\"type\":\"custom\",\"filter\":[\"lowercase\"]}}}},"
                + "\"mappings\":{\"properties\":{"
                + "\"t\":{\"type\":\"text\"},"
                + "\"tk\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}},"
                + "\"tn\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"normalizer\":\"lc\"}}},"
                + "\"ti\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":10}}},"
                + "\"k\":{\"type\":\"keyword\"},\"kn\":{\"type\":\"keyword\",\"normalizer\":\"lc\"},\"n\":{\"type\":\"long\"}}}}"
        );
        assertOkAndParse(client().performRequest(create), "create index");
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 70; i++) {
            String v = VALUES[i % VALUES.length];
            bulk.append("{\"index\":{}}\n{\"n\":").append(i);
            if (v != null) {
                for (String f : FIELDS) {
                    bulk.append(",\"").append(f).append("\":\"").append(v).append('"');
                }
            }
            bulk.append("}\n");
        }
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.addParameter("refresh", "true");
        req.setJsonEntity(bulk.toString());
        Map<String, Object> resp = assertOkAndParse(client().performRequest(req), "bulk");
        assertFalse("bulk errors: " + resp, Boolean.TRUE.equals(resp.get("errors")));
        provisioned = true;
    }

    @AfterClass
    public static void cleanUp() throws IOException {
        if (provisioned) {
            client().performRequest(new Request("DELETE", "/" + INDEX));
            provisioned = false;
        }
    }

    @Override
    public void tearDown() throws Exception {
        try {
            setModes(true, false);
        } finally {
            super.tearDown();
        }
    }

    public void testLuceneDriven() throws Exception {
        runAll(true, false, "lucene-driven");
    }

    public void testDataFusionDrivenWithPerformanceDelegation() throws Exception {
        runAll(false, false, "datafusion+perf");
    }

    public void testDataFusionWithLuceneBlocked() throws Exception {
        runAll(false, true, "datafusion, lucene blocked");
    }

    private void runAll(boolean preferLucene, boolean blockLucene, String mode) throws Exception {
        setModes(preferLucene, blockLucene);
        List<String> mismatches = new ArrayList<>();
        for (Map.Entry<String, Long> entry : VANILLA.entrySet()) {
            Map.Entry<String, Long> e = blockLucene && LUCENE_BLOCKED_OVERRIDES.containsKey(entry.getKey())
                ? Map.entry(entry.getKey(), LUCENE_BLOCKED_OVERRIDES.get(entry.getKey()))
                : entry;
            String ppl = "source=" + INDEX + " | where " + e.getKey() + " | stats count() as c";
            long got;
            try {
                @SuppressWarnings("unchecked")
                List<List<Object>> rows = (List<List<Object>>) executePpl(ppl).get("datarows");
                got = ((Number) rows.get(0).get(0)).longValue();
            } catch (Exception ex) {
                mismatches.add(e.getKey() + " -> ERROR " + ex.getMessage().split("\n")[0]);
                continue;
            }
            if (got != e.getValue()) {
                mismatches.add(e.getKey() + " -> vanilla=" + e.getValue() + " got=" + got);
            }
        }
        if (mismatches.isEmpty() == false) {
            fail(mode + ": " + mismatches.size() + " of " + VANILLA.size() + " differ from vanilla:\n  " + String.join("\n  ", mismatches));
        }
    }

    private void setModes(boolean preferLucene, boolean blockLucene) throws IOException {
        Request s = new Request("PUT", "/_cluster/settings");
        s.setJsonEntity(
            "{\"persistent\":{\"analytics.planner.prefer_metadata_driver\":"
                + preferLucene
                + ",\"analytics.delegation.lucene.blocked_predicates\":"
                + (blockLucene ? BLOCK_ALL : "[]")
                + "}}"
        );
        client().performRequest(s);
    }
}
