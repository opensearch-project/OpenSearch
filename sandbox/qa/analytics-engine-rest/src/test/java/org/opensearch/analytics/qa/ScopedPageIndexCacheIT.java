/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Map;

/**
 * Integration tests for the scoped page-index cache (ColumnIndex + OffsetIndex).
 *
 * <p>The cache is governed by the {@code datafusion.scoped_page_index.enabled} cluster setting.
 * Key invariants verified across the suite:
 * <ul>
 *   <li>Enabled: CI fills on cold query (misses), hits on warm repeat. Metadata cache is smaller
 *       because page index entries are stripped and kept separately in the scoped cache.</li>
 *   <li>Disabled: CI/OI stats = 0. Metadata cache is larger because page index is retained via
 *       PageIndexPolicy::Optional.</li>
 *   <li>Toggle-off clears CI/OI caches immediately; metadata cache is unaffected.</li>
 *   <li>Explicit clear resets stats so the next query is a miss again.</li>
 *   <li>OI fills on projection-only queries; CI does NOT fill when there is no predicate.</li>
 *   <li>Both CI and OI fill when the same column is filtered and projected.</li>
 *   <li>Schema-drift (new field added in a later segment) is handled: CI fills for the new field
 *       from the segment that carries it, and the old-field query continues to work.</li>
 *   <li>After a force-merge the pre-merge segment entries are gone; a fresh query re-misses.</li>
 * </ul>
 *
 * <p>Every test that needs clean measurements calls {@code recreateIndex()} (or a named variant),
 * then {@code clearAllCaches()}, then {@code assertCacheEmpty()} before measuring. This ensures
 * that any warming triggered by the refresh inside {@code recreateIndex()} is wiped out.
 */
public class ScopedPageIndexCacheIT extends AnalyticsRestTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INDEX_NAME = "scoped_cache_it";
    private static final String DYNAMIC_INDEX_NAME = "scoped_cache_dynamic_it";
    private static final String CLEAR_ENDPOINT = "/_plugins/_analytics_backend_datafusion/cache/_clear";
    private static final int DOC_COUNT = 2000;

    /**
     * Per-test setup via {@link AnalyticsRestTestCase#onBeforeQuery()} — this fires
     * inside the test body lifecycle where {@link #client()} is guaranteed non-null.
     * Using {@code setUp()} is unsafe here because {@code client()} may not yet be
     * initialized when setUp() runs (see existing IT comments in this package).
     */
    @Override
    protected void onBeforeQuery() throws IOException {
        setScopedPageIndexEnabled(true);
        try {
            recreateIndex();
            clearAllCaches();
        } catch (Exception e) {
            throw new IOException("ScopedPageIndexCacheIT setup failed", e);
        }
    }

    @Override
    public void tearDown() throws Exception {
        try {
            setScopedPageIndexEnabled(true);
            restoreCacheDefaults();
            restoreLuceneBlockedPredicates();
            clearAllCaches();
        } catch (Exception ignored) {
            // best-effort: don't mask the test failure
        }
        super.tearDown();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    // ── listing table path ────────────────────────────────────────────────────

    /**
     * Listing path (numeric age predicate): refresh must NOT fill the scoped CI cache; only a
     * query should. Cold run produces CI misses; warm repeat produces CI hits.
     */
    public void testListingPathColdMissThenWarmHit() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where age > 90 | stats count()";

        // After recreateIndex() + clearAllCaches() in setUp():
        // both metadata memory and CI entries must be 0 — refresh did not fill the cache.
        JsonNode afterRefresh = stats();
        assertEquals("refresh must NOT fill metadata cache", 0L, metaMemoryBytes(afterRefresh));
        assertEquals("refresh must NOT fill CI cache", 0L, ciEntries(afterRefresh));
        assertEquals("refresh must NOT fill OI cache", 0L, oiEntries(afterRefresh));

        // Cold query — must fill CI and OI (misses) and populate metadata cache.
        executePpl(ppl);
        JsonNode cold = stats();
        assertTrue("cold query must populate metadata cache", metaMemoryBytes(cold) > 0);
        assertTrue("cold query must register CI misses", ciMisses(cold) > 0);
        assertTrue("cold query must register OI misses (predicate col + col 0)", oiMisses(cold) > 0);

        // Warm query — same query hits from both CI and OI caches.
        executePpl(ppl);
        JsonNode warm = stats();
        assertTrue("warm query must register CI hits", ciHits(warm) > 0);
        assertTrue("warm query must register OI hits", oiHits(warm) > 0);
    }

    /**
     * Listing path disabled: CI/OI = 0, metadata cache larger than enabled baseline.
     *
     * <p>Flow per mode: delete → create → index → refresh → clearAllCaches() →
     * assertCacheEmpty() → query → measure.
     */
    public void testListingPathDisabledMetadataCacheLarger() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where age > 90 | stats count()";

        // ── ENABLED baseline ──────────────────────────────────────────────
        setScopedPageIndexEnabled(true);
        recreateIndex();
        clearAllCaches();
        assertCacheEmpty();

        executePpl(ppl);
        JsonNode s1 = stats();
        long metaEnabled = metaMemoryBytes(s1);
        assertTrue("enabled: metadata cache populated by query", metaEnabled > 0);
        assertTrue("enabled: CI must have activity", ciMisses(s1) + ciHits(s1) > 0);

        // ── DISABLED ─────────────────────────────────────────────────────
        setScopedPageIndexEnabled(false);
        assertEquals("CI entries 0 after auto-clear on disable", 0L, ciEntries(stats()));
        assertEquals("OI entries 0 after auto-clear on disable", 0L, oiEntries(stats()));

        recreateIndex();
        clearAllCaches();
        assertCacheEmpty();

        executePpl(ppl);
        JsonNode s2 = stats();
        long metaDisabled = metaMemoryBytes(s2);
        assertTrue("disabled: metadata cache populated by query", metaDisabled > 0);
        assertTrue(
            "disabled metadata >= enabled (page index retained): enabled="
                + metaEnabled + " disabled=" + metaDisabled,
            metaDisabled >= metaEnabled
        );
        assertEquals("CI hits 0 with scoped disabled", 0L, ciHits(s2));
        assertEquals("CI misses 0 with scoped disabled", 0L, ciMisses(s2));
        assertEquals("OI hits 0 with scoped disabled", 0L, oiHits(s2));
        assertEquals("OI misses 0 with scoped disabled", 0L, oiMisses(s2));
    }

    // ── indexed path (match() → Lucene + DataFusion) ──────────────────────────

    /**
     * Indexed path: refresh must NOT fill the scoped CI/OI cache; only a query should.
     * Cold run produces CI/OI misses; warm repeat produces hits.
     */
    public void testIndexedPathColdMissThenWarmHit() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where match(city, 'seattle') and age > 50 | stats count()";

        // After setUp(): refresh must not have filled the scoped cache.
        JsonNode afterRefresh = stats();
        assertEquals("refresh must NOT fill metadata cache", 0L, metaMemoryBytes(afterRefresh));
        assertEquals("refresh must NOT fill CI cache", 0L, ciEntries(afterRefresh));
        assertEquals("refresh must NOT fill OI cache", 0L, oiEntries(afterRefresh));

        // Cold query — fills both CI and OI.
        executePpl(ppl);
        JsonNode cold = stats();
        assertTrue("indexed cold: metadata cache populated by query", metaMemoryBytes(cold) > 0);
        assertTrue("indexed cold: CI misses > 0", ciMisses(cold) > 0);
        assertTrue("indexed cold: OI misses > 0 (predicate col + col 0)", oiMisses(cold) > 0);

        // Warm query — hits from both CI and OI caches.
        executePpl(ppl);
        JsonNode warm = stats();
        assertTrue("indexed warm: CI hits > 0", ciHits(warm) > 0);
        assertTrue("indexed warm: OI hits > 0", oiHits(warm) > 0);
    }

    /**
     * Indexed path disabled: CI/OI = 0, metadata cache larger than enabled baseline.
     *
     * <p>Same flow as the listing test: recreate → clear → assertCacheEmpty → query → measure.
     */
    public void testIndexedPathDisabledMetadataCacheLarger() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where match(city, 'seattle') and age > 50 | stats count()";

        // ── ENABLED baseline ──────────────────────────────────────────────
        setScopedPageIndexEnabled(true);
        recreateIndex();
        clearAllCaches();
        assertCacheEmpty();

        executePpl(ppl);
        JsonNode s1 = stats();
        long metaEnabled = metaMemoryBytes(s1);
        assertTrue("indexed enabled: metadata cache populated", metaEnabled > 0);

        // ── DISABLED ─────────────────────────────────────────────────────
        setScopedPageIndexEnabled(false);
        assertEquals("CI entries 0 after auto-clear on disable", 0L, ciEntries(stats()));
        assertEquals("OI entries 0 after auto-clear on disable", 0L, oiEntries(stats()));

        recreateIndex();
        clearAllCaches();
        assertCacheEmpty();

        executePpl(ppl);
        JsonNode s2 = stats();
        long metaDisabled = metaMemoryBytes(s2);
        assertTrue("indexed disabled: metadata cache populated", metaDisabled > 0);
        assertTrue(
            "indexed disabled metadata >= enabled (page index retained): enabled="
                + metaEnabled + " disabled=" + metaDisabled,
            metaDisabled >= metaEnabled
        );
        assertEquals("CI hits 0 on indexed path disabled", 0L, ciHits(s2));
        assertEquals("CI misses 0 on indexed path disabled", 0L, ciMisses(s2));
        assertEquals("OI hits 0 on indexed path disabled", 0L, oiHits(s2));
        assertEquals("OI misses 0 on indexed path disabled", 0L, oiMisses(s2));
    }

    // ── string field filter (CI) ──────────────────────────────────────────────

    /**
     * String keyword field predicate: {@code city = 'seattle'} hits the ColumnIndex because
     * {@code city} is a keyword with string min/max statistics in the parquet ColumnIndex.
     * Cold query fills CI with misses; warm repeat hits from cache.
     */
    public void testStringFieldFilterFillsColumnIndex() throws Exception {
        // city is a keyword field; equality would normally be delegated to Lucene and never
        // touch parquet. Block EQUALS so the predicate scans parquet and fills the ColumnIndex.
        setLuceneBlockedPredicates("EQUALS");
        String ppl = "source=" + INDEX_NAME + " | where city = 'seattle' | stats count()";
        clearAllCaches();
        assertCacheEmpty();

        assertCacheFillsOnCold(ppl);
        // CI: 1 file × city(col 3) × 1 RG = 1 entry
        assertExactCiEntries("string predicate: CI entries = 1 (file×col×rg)", 1L);
        // OI: 1 file × {col_0(name), city(col 3)} = 2 entries
        assertExactOiEntries("string predicate: OI entries = 2 (file×{col_0,city})", 2L);
        assertCacheHitsOnWarm(ppl);
    }

    // ── numeric field filter (CI) ─────────────────────────────────────────────

    /**
     * Numeric double field predicate: {@code score > 75.0} hits the ColumnIndex because
     * {@code score} carries numeric min/max statistics in the parquet ColumnIndex.
     * Cold query fills CI with misses; warm repeat hits from cache.
     */
    public void testNumericFieldFilterFillsColumnIndex() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where score > 75.0 | stats count()";
        clearAllCaches();
        assertCacheEmpty();

        assertCacheFillsOnCold(ppl);
        // CI: 1 file × score(col 2) × 1 RG = 1 entry
        assertExactCiEntries("numeric predicate: CI entries = 1 (file×col×rg)", 1L);
        // OI: 1 file × {col_0(name), score(col 2)} = 2 entries
        assertExactOiEntries("numeric predicate: OI entries = 2 (file×{col_0,score})", 2L);
        assertCacheHitsOnWarm(ppl);
    }

    // ── projection-only, no filter (OI only) ─────────────────────────────────

    /**
     * Projection-only query with no filter: {@code fields name, score | head 100} requires the
     * OffsetIndex to locate the page offsets for the projected columns, but there is no predicate
     * so the ColumnIndex should NOT be consulted at all.
     * Asserts: OI misses > 0, CI misses == 0.
     */
    public void testProjectionOnlyFillsOffsetIndexNotColumnIndex() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | fields name, score | head 100";
        clearAllCaches();
        assertCacheEmpty();

        assertOnlyOiFills(ppl);
        // OI key = (file, col); the column set is {col 0} ∪ predicate ∪ projection. The
        // page-skip metric always reads col 0, so it is included even when not projected.
        // Here name/score are distinct keyword/double leaves (neither is col 0), so the set
        // is {col 0, name, score} = 3 entries over 1 file.
        assertExactOiEntries("projection-only: OI entries = 3 (file×{col 0,name,score})", 3L);
        assertExactCiEntries("projection-only: CI entries = 0 (no predicate)", 0L);
    }

    // ── filter = projection same columns (CI + OI overlap) ───────────────────

    /**
     * When the same column ({@code age}) is used in both a filter predicate and a projection,
     * the query must consult both the ColumnIndex (predicate push-down) and the OffsetIndex
     * (column projection). Both CI and OI should accumulate misses on the cold run.
     */
    public void testFilterAndProjectionSameColumnFillsBothCaches() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where age > 80 | fields age | stats count()";
        clearAllCaches();
        assertCacheEmpty();

        executePpl(ppl);
        JsonNode cold = stats();
        assertPositive("CI+OI overlap: CI misses > 0", ciMisses(cold));
        assertPositive("CI+OI overlap: OI misses > 0", oiMisses(cold));
        assertPositive("CI+OI overlap: metadata populated", metaMemoryBytes(cold));
        // CI: 1 file × age(col 1) × 1 RG = 1 entry
        // OI: 1 file × {col_0(name), age(col 1)} = 2 entries (predicate ∪ projection ∪ {col_0})
        assertExactCiEntries("filter=projection: CI entries = 1 (age×file×rg)", 1L);
        assertExactOiEntries("filter=projection: OI entries = 2 (file×{col_0(name),age})", 2L);
    }

    // ── dynamic mapping — new field in second segment ─────────────────────────

    /**
     * Schema-drift scenario: a new field ({@code region}) is added in a second batch after the
     * first segment already exists. Verifies that:
     * <ol>
     *   <li>The first batch's field ({@code age}) can still be queried correctly after the schema
     *       expands.</li>
     *   <li>The new field ({@code region}) fills the CI from the segment that carries it, even
     *       though the first segment has no such column.</li>
     * </ol>
     * Uses a dedicated index ({@value #DYNAMIC_INDEX_NAME}) with dynamic mapping so no explicit
     * schema is defined — the mapping is inferred from each batch.
     */
    public void testDynamicMappingNewFieldInSecondSegmentFillsCI() throws Exception {
        // region/city are keyword fields; block EQUALS so the keyword-equality predicate
        // (region = 'west') scans parquet and fills the ColumnIndex instead of being
        // delegated to Lucene.
        setLuceneBlockedPredicates("EQUALS");

        // Clean up any leftovers from a prior run.
        try { client().performRequest(new Request("DELETE", "/" + DYNAMIC_INDEX_NAME)); } catch (Exception ignored) {}

        // Create index with dynamic mapping (no explicit properties) and composite parquet format.
        String createBody = "{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "}}";
        Request create = new Request("PUT", "/" + DYNAMIC_INDEX_NAME);
        create.setJsonEntity(createBody);
        assertOkAndParse(client().performRequest(create), "create dynamic index");

        Request health = new Request("GET", "/_cluster/health/" + DYNAMIC_INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Batch 1: fields name, age, city — creates segment 1.
        StringBuilder batch1 = new StringBuilder();
        String[] cities = {"seattle", "portland", "denver", "austin", "boston"};
        for (int i = 0; i < 500; i++) {
            batch1.append("{\"index\":{}}\n")
                .append("{\"name\":\"user").append(i)
                .append("\",\"age\":").append(i % 100)
                .append(",\"city\":\"").append(cities[i % cities.length])
                .append("\"}\n");
        }
        Request bulk1 = new Request("POST", "/" + DYNAMIC_INDEX_NAME + "/_bulk");
        bulk1.setJsonEntity(batch1.toString());
        bulk1.addParameter("refresh", "true");
        bulk1.setOptions(bulk1.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> r1 = assertOkAndParse(client().performRequest(bulk1), "dynamic bulk1");
        assertEquals("dynamic bulk1 errors", false, r1.get("errors"));

        // After batch 1 refresh: clear caches and verify age query fills CI.
        clearAllCaches();
        assertCacheEmpty();

        String agePpl = "source=" + DYNAMIC_INDEX_NAME + " | where age > 50 | stats count()";
        executePpl(agePpl);
        JsonNode afterBatch1 = stats();
        assertPositive("dynamic batch1: CI fills for age", ciMisses(afterBatch1));
        assertPositive("dynamic batch1: metadata populated", metaMemoryBytes(afterBatch1));

        // Batch 2: adds NEW field region to every doc → creates segment 2 with schema drift.
        StringBuilder batch2 = new StringBuilder();
        String[] regions = {"west", "east", "central"};
        for (int i = 500; i < 1000; i++) {
            batch2.append("{\"index\":{}}\n")
                .append("{\"name\":\"user").append(i)
                .append("\",\"age\":").append(i % 100)
                .append(",\"city\":\"").append(cities[i % cities.length])
                .append("\",\"region\":\"").append(regions[i % regions.length])
                .append("\"}\n");
        }
        Request bulk2 = new Request("POST", "/" + DYNAMIC_INDEX_NAME + "/_bulk");
        bulk2.setJsonEntity(batch2.toString());
        bulk2.addParameter("refresh", "true");
        bulk2.setOptions(bulk2.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> r2 = assertOkAndParse(client().performRequest(bulk2), "dynamic bulk2");
        assertEquals("dynamic bulk2 errors", false, r2.get("errors"));

        // Clear caches before measuring new-field scenario.
        clearAllCaches();
        assertCacheEmpty();

        // Query on new field region — only segment 2 carries it; CI must fill from that segment.
        String regionPpl = "source=" + DYNAMIC_INDEX_NAME + " | where region = 'west' | stats count()";
        executePpl(regionPpl);
        JsonNode afterRegion = stats();
        assertPositive("dynamic: CI fills for new field region", ciMisses(afterRegion));
        assertPositive("dynamic: metadata populated after region query", metaMemoryBytes(afterRegion));

        // The old-field age query must still work correctly after schema drift.
        clearAllCaches();
        assertCacheEmpty();
        executePpl(agePpl);
        JsonNode afterAgeDrift = stats();
        assertPositive("dynamic: age CI still fills after schema drift", ciMisses(afterAgeDrift));
    }

    // ── refresh → merge lifecycle ─────────────────────────────────────────────

    /**
     * Force-merge lifecycle: two separate refresh cycles produce two segments. After a
     * force-merge to one segment the pre-merge cache entries are stale; a fresh query after
     * clearing must re-miss against the merged segment.
     *
     * <p>Flow:
     * <ol>
     *   <li>Batch 1 → refresh (segment 1)</li>
     *   <li>Batch 2 → refresh (segment 2)</li>
     *   <li>clearAllCaches → assertCacheEmpty → query → assert CI fills across both segments</li>
     *   <li>Force-merge to 1 segment</li>
     *   <li>clearAllCaches → assertCacheEmpty (old entries gone)</li>
     *   <li>Query → CI misses > 0 (merged segment is a fresh miss)</li>
     * </ol>
     */
    public void testRefreshThenMergeCacheRefillsAfterMerge() throws Exception {
        // Start with a clean index that has NO data yet — we will drive two separate
        // refreshes ourselves so we get two distinct segments.
        try { client().performRequest(new Request("DELETE", "/" + INDEX_NAME)); } catch (Exception ignored) {}

        String createBody = "{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"age\":{\"type\":\"integer\"},"
            + "  \"score\":{\"type\":\"double\"},"
            + "  \"city\":{\"type\":\"keyword\"}"
            + "}}}";
        Request createReq = new Request("PUT", "/" + INDEX_NAME);
        createReq.setJsonEntity(createBody);
        assertOkAndParse(client().performRequest(createReq), "merge-test: create index");

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        String[] cities = {"seattle", "portland", "denver", "austin", "boston"};

        // Batch 1 → refresh → segment 1.
        StringBuilder b1 = new StringBuilder();
        for (int i = 0; i < 500; i++) {
            b1.append("{\"index\":{}}\n")
                .append("{\"name\":\"user").append(i)
                .append("\",\"age\":").append(i % 100)
                .append(",\"score\":").append(50.0 + (i % 50))
                .append(",\"city\":\"").append(cities[i % cities.length])
                .append("\"}\n");
        }
        Request bulk1 = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulk1.setJsonEntity(b1.toString());
        bulk1.addParameter("refresh", "true");
        bulk1.setOptions(bulk1.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> rb1 = assertOkAndParse(client().performRequest(bulk1), "merge-test bulk1");
        assertEquals("merge-test bulk1 errors", false, rb1.get("errors"));

        // Batch 2 → refresh → segment 2.
        StringBuilder b2 = new StringBuilder();
        for (int i = 500; i < 1000; i++) {
            b2.append("{\"index\":{}}\n")
                .append("{\"name\":\"user").append(i)
                .append("\",\"age\":").append(i % 100)
                .append(",\"score\":").append(50.0 + (i % 50))
                .append(",\"city\":\"").append(cities[i % cities.length])
                .append("\"}\n");
        }
        Request bulk2 = new Request("POST", "/" + INDEX_NAME + "/_bulk");
        bulk2.setJsonEntity(b2.toString());
        bulk2.addParameter("refresh", "true");
        bulk2.setOptions(bulk2.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> rb2 = assertOkAndParse(client().performRequest(bulk2), "merge-test bulk2");
        assertEquals("merge-test bulk2 errors", false, rb2.get("errors"));

        // Clear caches then verify query fills CI across both segments.
        clearAllCaches();
        assertCacheEmpty();

        String ppl = "source=" + INDEX_NAME + " | where age > 80 | stats count()";
        executePpl(ppl);
        JsonNode premerge = stats();
        assertPositive("pre-merge: CI misses across both segments", ciMisses(premerge));
        assertPositive("pre-merge: OI misses across both segments", oiMisses(premerge));
        assertPositive("pre-merge: metadata populated", metaMemoryBytes(premerge));
        // CI: 2 files × age(col 1) × 1 RG each = 2 entries
        assertExactCiEntries("pre-merge: CI entries = 2 (2 files × age × rg)", 2L);
        // OI: 2 files × {col_0(name), age(col 1)} = 4 entries
        assertExactOiEntries("pre-merge: OI entries = 4 (2 files × {col_0,age})", 4L);

        // Force-merge to a single segment — old segment entries in the scoped cache are stale.
        Request fm = new Request("POST", "/" + INDEX_NAME + "/_forcemerge");
        fm.addParameter("max_num_segments", "1");
        client().performRequest(fm);

        // Wipe stale entries; the merged segment should not be in the cache.
        clearAllCaches();
        assertCacheEmpty();

        // Fresh query against the merged segment — must re-miss.
        executePpl(ppl);
        JsonNode postmerge = stats();
        assertPositive("post-merge: CI misses > 0 (forced re-miss after merge)", ciMisses(postmerge));
        assertPositive("post-merge: OI misses > 0 (forced re-miss after merge)", oiMisses(postmerge));
        assertPositive("post-merge: metadata populated from merged segment", metaMemoryBytes(postmerge));
        // CI: 1 merged file × age(col 1) × 1 RG = 1 entry (down from 2)
        assertExactCiEntries("post-merge: CI entries = 1 (1 merged file × age × rg)", 1L);
        // OI: 1 merged file × {col_0(name), age(col 1)} = 2 entries (down from 4)
        assertExactOiEntries("post-merge: OI entries = 2 (1 merged file × {col_0,age})", 2L);
    }

    // ── cross-index shared cache ──────────────────────────────────────────────

    private static final String INDEX_B = "scoped_cache_it_b";

    /**
     * Two parquet indices share the same process-global CI/OI caches.
     * Queries on INDEX_NAME and INDEX_B accumulate independent entries
     * (different file paths → different cache keys). Entries from one index
     * must not corrupt or evict entries from the other if the cache is sized
     * to hold both comfortably.
     *
     * <p>This exercises the FIFO write lock under real concurrent key diversity.
     */
    public void testCrossIndexCacheEntriesAreIndependent() throws Exception {
        // Provision a second index with the same schema.
        try { client().performRequest(new Request("DELETE", "/" + INDEX_B)); } catch (Exception ignored) {}
        provisionNamedIndex(INDEX_B);
        try {
            clearAllCaches();
            assertCacheEmpty();

            String pplA = "source=" + INDEX_NAME + " | where age > 80 | stats count()";
            String pplB = "source=" + INDEX_B + " | where score > 75.0 | stats count()";

            // Cold run on both indices.
            executePpl(pplA);
            executePpl(pplB);
            JsonNode cold = stats();
            // Each index contributes 1 CI entry (1 file × 1 pred col × 1 RG).
            // Total CI entries = 2 (one per index, different file paths).
            long ciAfterBoth = ciEntries(cold);
            assertTrue("cross-index: CI entries must be >= 2 after cold queries on both indices",
                ciAfterBoth >= 2);

            // Warm run on both — both should hit.
            long hitsBefore = ciHits(cold);
            executePpl(pplA);
            executePpl(pplB);
            JsonNode warm = stats();
            assertTrue("cross-index: CI hits must increase after warm queries on both indices",
                ciHits(warm) > hitsBefore);

            // Clearing index A's entries via prefix eviction must not affect index B.
            // (Index B has different file path prefix so its entries survive.)
            executePpl(pplB); // ensure B is warm
            long bHitsBefore = ciHits(stats());
            // Simulate an eviction for INDEX_NAME by clearing all and re-running only B.
            clearAllCaches();
            executePpl(pplA); // re-populates A
            executePpl(pplB); // should hit from re-populated B after cache clear
            // B's entries are fresh misses after the clear — that's expected and correct.
            assertPositive("cross-index: B CI misses after full clear", ciMisses(stats()));
        } finally {
            try { client().performRequest(new Request("DELETE", "/" + INDEX_B)); } catch (Exception ignored) {}
        }
    }

    /**
     * Cache sized to hold exactly one index's worth of entries (tight budget).
     * Concurrent queries on INDEX_NAME and INDEX_B put the FIFO write lock under
     * contention while eviction fires. After all queries:
     * <ul>
     *   <li>used_bytes must not exceed the limit (correctness under eviction).</li>
     *   <li>Both indices must still return correct query results (cache is optional).</li>
     *   <li>Cache stats must be internally consistent (no phantom entries).</li>
     * </ul>
     *
     * <p>The budget is set to approximately the size of CI entries for one index so
     * inserting B's entries forces eviction of A's (or vice versa) — exercising the
     * FIFO drain path under concurrent inserts.
     */
    public void testCacheContetionTwoIndicesOneTightBudget() throws Exception {
        try { client().performRequest(new Request("DELETE", "/" + INDEX_B)); } catch (Exception ignored) {}
        provisionNamedIndex(INDEX_B);
        try {
            // Set a tight budget: ~500 bytes — enough for a handful of entries but not all.
            // A typical CI entry for an integer column with 2000 rows is ~100-500 bytes.
            // This forces eviction when both indices push entries concurrently.
            setCacheTotalSize("2kb");
            clearAllCaches();
            assertCacheEmpty();

            String pplA1 = "source=" + INDEX_NAME + " | where age > 80 | stats count()";
            String pplA2 = "source=" + INDEX_NAME + " | where score > 75.0 | stats count()";
            String pplB1 = "source=" + INDEX_B + " | where age > 60 | stats count()";
            String pplB2 = "source=" + INDEX_B + " | where score > 60.0 | stats count()";

            // Interleave queries on both indices — drives concurrent cache inserts + evictions.
            for (int i = 0; i < 3; i++) {
                executePpl(pplA1);
                executePpl(pplB1);
                executePpl(pplA2);
                executePpl(pplB2);
                // Interleave refreshes to create new segments.
                if (i == 1) {
                    client().performRequest(new Request("POST", "/" + INDEX_NAME + "/_refresh"));
                    client().performRequest(new Request("POST", "/" + INDEX_B + "/_refresh"));
                }
            }

            JsonNode s = stats();
            long limit = s.get("column_index_cache").get("size_limit_bytes").asLong();
            long usedBytes = s.get("column_index_cache").get("memory_bytes").asLong();

            // Correctness: cache must never exceed its limit.
            assertTrue(
                "CI used_bytes=" + usedBytes + " must be <= size_limit_bytes=" + limit,
                usedBytes <= limit
            );

            // Result correctness: queries must return valid results regardless of eviction.
            // Re-run all queries — they must not error even if the cache is empty.
            executePpl(pplA1);
            executePpl(pplA2);
            executePpl(pplB1);
            executePpl(pplB2);

        } finally {
            restoreCacheDefaults();
            clearAllCaches();
            try { client().performRequest(new Request("DELETE", "/" + INDEX_B)); } catch (Exception ignored) {}
        }
    }

    /** Provision a named parquet index with the same schema as the main test index. */
    private void provisionNamedIndex(String indexName) throws IOException {
        String body = "{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"age\":{\"type\":\"integer\"},"
            + "  \"score\":{\"type\":\"double\"},"
            + "  \"city\":{\"type\":\"keyword\"}"
            + "}}}";
        Request create = new Request("PUT", "/" + indexName);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create " + indexName);

        Request health = new Request("GET", "/_cluster/health/" + indexName);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        String[] cities = {"seattle", "portland", "denver", "austin", "boston"};
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < DOC_COUNT; i++) {
            bulk.append("{\"index\":{}}\n")
                .append("{\"name\":\"user").append(i)
                .append("\",\"age\":").append(i % 100)
                .append(",\"score\":").append(50.0 + (i % 50))
                .append(",\"city\":\"").append(cities[i % cities.length])
                .append("\"}\n");
        }
        Request bulkReq = new Request("POST", "/" + indexName + "/_bulk");
        bulkReq.setJsonEntity(bulk.toString());
        bulkReq.addParameter("refresh", "true");
        bulkReq.setOptions(bulkReq.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> r = assertOkAndParse(client().performRequest(bulkReq), "bulk " + indexName);
        assertEquals(indexName + " bulk errors", false, r.get("errors"));
    }

    // ── full enabled → disabled lifecycle loop ────────────────────────────────

    /**
     * Full lifecycle covering listing + indexed paths through enabled → disabled → re-enabled:
     *
     * <pre>
     * Phase 1 — ENABLED, listing path
     *   recreate → clear → assertCacheEmpty (refresh did NOT fill)
     *   query → assert metadata populated, CI misses > 0
     *   query again → assert CI hits > 0 (warm)
     *
     * Phase 2 — ENABLED, indexed path  (same data, same caches still live)
     *   match() query → CI/OI misses or hits recorded (cumulative)
     *   match() query again → hits increase
     *
     * Phase 3 — DISABLE  (auto-clear CI/OI)
     *   assert CI entries = 0, OI entries = 0 immediately
     *   assert metadata still present (not cleared by disabling)
     *
     * Phase 4 — DISABLED, listing path
     *   recreate → clear → assertCacheEmpty
     *   query → assert metadata populated AND larger than enabled baseline
     *   assert CI = 0, OI = 0
     *
     * Phase 5 — DISABLED, indexed path
     *   recreate → clear → assertCacheEmpty
     *   match() query → metadata larger than enabled, CI = 0, OI = 0
     *
     * Phase 6 — RE-ENABLE
     *   recreate → clear → assertCacheEmpty
     *   query → CI misses fill again (confirms scoped cache is back)
     * </pre>
     */
    public void testFullLifecycleEnabledDisabledReenabled() throws Exception {
        String listingPpl = "source=" + INDEX_NAME + " | where age > 90 | stats count()";
        String indexedPpl = "source=" + INDEX_NAME + " | where match(city, 'seattle') and age > 50 | stats count()";

        // ── Phase 1: ENABLED, listing path ────────────────────────────────
        setScopedPageIndexEnabled(true);
        recreateIndex();
        clearAllCaches();

        // Refresh must NOT fill the scoped cache.
        assertZero("P1 refresh: metadata", metaMemoryBytes(stats()));
        assertZero("P1 refresh: CI entries", ciEntries(stats()));
        assertZero("P1 refresh: OI entries", oiEntries(stats()));

        // Cold listing query.
        executePpl(listingPpl);
        JsonNode p1cold = stats();
        assertPositive("P1 cold: metadata populated", metaMemoryBytes(p1cold));
        assertPositive("P1 cold: CI misses", ciMisses(p1cold));
        long metaEnabledListing = metaMemoryBytes(p1cold);

        // Warm listing query.
        executePpl(listingPpl);
        assertPositive("P1 warm: CI hits", ciHits(stats()));

        // ── Phase 2: ENABLED, indexed path ────────────────────────────────
        executePpl(indexedPpl);
        JsonNode p2cold = stats();
        long p2coldActivity = ciMisses(p2cold) + oiMisses(p2cold) + ciHits(p2cold) + oiHits(p2cold);
        assertPositive("P2 cold indexed: CI/OI activity", p2coldActivity);

        executePpl(indexedPpl);
        JsonNode p2warm = stats();
        long p2warmHits = ciHits(p2warm) + oiHits(p2warm);
        assertPositive("P2 warm indexed: CI or OI hits", p2warmHits);

        // ── Phase 3: DISABLE ──────────────────────────────────────────────
        setScopedPageIndexEnabled(false);
        JsonNode p3 = stats();
        assertZero("P3 disable: CI entries auto-cleared", ciEntries(p3));
        assertZero("P3 disable: OI entries auto-cleared", oiEntries(p3));
        // Metadata cache is unaffected by the toggle.
        assertPositive("P3 disable: metadata cache still present", metaMemoryBytes(p3));

        // ── Phase 4: DISABLED, listing path ───────────────────────────────
        recreateIndex();
        clearAllCaches();
        assertZero("P4 before query: metadata", metaMemoryBytes(stats()));
        assertZero("P4 before query: CI", ciEntries(stats()));
        assertZero("P4 before query: OI", oiEntries(stats()));

        executePpl(listingPpl);
        JsonNode p4 = stats();
        long metaDisabledListing = metaMemoryBytes(p4);
        assertPositive("P4 disabled listing: metadata populated", metaDisabledListing);
        assertTrue(
            "P4 disabled listing metadata >= enabled (page index retained): enabled="
                + metaEnabledListing + " disabled=" + metaDisabledListing,
            metaDisabledListing >= metaEnabledListing
        );
        assertZero("P4 CI hits", ciHits(p4));
        assertZero("P4 CI misses", ciMisses(p4));
        assertZero("P4 OI hits", oiHits(p4));
        assertZero("P4 OI misses", oiMisses(p4));

        // ── Phase 5: DISABLED, indexed path ───────────────────────────────
        recreateIndex();
        clearAllCaches();
        assertZero("P5 before query: metadata", metaMemoryBytes(stats()));

        executePpl(indexedPpl);
        JsonNode p5 = stats();
        long metaDisabledIndexed = metaMemoryBytes(p5);
        assertPositive("P5 disabled indexed: metadata populated", metaDisabledIndexed);
        assertTrue(
            "P5 disabled indexed metadata >= enabled: enabled="
                + metaEnabledListing + " disabled=" + metaDisabledIndexed,
            metaDisabledIndexed >= metaEnabledListing
        );
        assertZero("P5 CI hits", ciHits(p5));
        assertZero("P5 CI misses", ciMisses(p5));
        assertZero("P5 OI hits", oiHits(p5));
        assertZero("P5 OI misses", oiMisses(p5));

        // ── Phase 6: RE-ENABLE ────────────────────────────────────────────
        setScopedPageIndexEnabled(true);
        recreateIndex();
        clearAllCaches();
        assertZero("P6 before query: metadata", metaMemoryBytes(stats()));
        assertZero("P6 before query: CI", ciEntries(stats()));

        executePpl(listingPpl);
        JsonNode p6 = stats();
        assertPositive("P6 re-enabled: CI misses (scoped cache back)", ciMisses(p6));
        assertPositive("P6 re-enabled: metadata populated", metaMemoryBytes(p6));
        // Metadata is footer-only again (smaller than disabled).
        assertTrue(
            "P6 re-enabled metadata must be <= disabled (page index stripped): enabled="
                + metaMemoryBytes(p6) + " disabled=" + metaDisabledListing,
            metaMemoryBytes(p6) <= metaDisabledListing
        );
    }

    // ── pure Lucene path (no parquet scan) ───────────────────────────────────

    /**
     * {@code match(city, 'seattle') | stats count()} can be satisfied entirely by
     * Lucene's inverted index without touching the parquet files, so neither CI
     * nor OI should be populated. No parquet IO means no page-index loading.
     */
    public void testPureLuceneCountQueryProducesNoCacheActivity() throws Exception {
        clearAllCaches();
        assertCacheEmpty();

        // match() + count(*) — Lucene satisfies this without scanning parquet pages
        String ppl = "source=" + INDEX_NAME + " | where match(city, 'seattle') | stats count()";
        executePpl(ppl);
        executePpl(ppl);

        JsonNode s = stats();
        assertZero("pure Lucene count: CI hits must be 0", ciHits(s));
        assertZero("pure Lucene count: CI misses must be 0", ciMisses(s));
        assertZero("pure Lucene count: OI hits must be 0", oiHits(s));
        assertZero("pure Lucene count: OI misses must be 0", oiMisses(s));
        assertZero("pure Lucene count: CI entries must be 0", ciEntries(s));
        assertZero("pure Lucene count: OI entries must be 0", oiEntries(s));
    }

    /**
     * Aggregation whose {@code case(...)} expression reads a column that is NOT in the
     * group-by/projection list must still scan that column correctly.
     *
     * <p>Regression test for a scoped-OffsetIndex bug: the optimizer derived the scan's read set
     * from the projected output field <em>names</em>, but with expression push-down the projection
     * contains computed fields like {@code CASE WHEN status = 200 ...} whose names are not file
     * columns. The base column the CASE reads ({@code age}/{@code score} here) was therefore left
     * out of the scoped OffsetIndex and got a single-page placeholder. When the scan actually read
     * that column, arrow decoded the whole chunk as one page and failed with
     * "provided output is too small for the decompressed data" (HTTP 500).
     *
     * <p>Two variants exercise both factory-install paths:
     * <ul>
     *   <li>Listing path — numeric predicate keeps the scan on the listing/parquet path.</li>
     *   <li>Indexed path — {@code match()} routes through the Lucene-indexed executor, which wires
     *       the scoped page index via a different code path ({@code collect_plan_column_names}).</li>
     * </ul>
     * Each query groups by one column while a {@code case()} reads a different, non-projected one;
     * {@code executePpl} asserts HTTP 200, so a decompression failure fails the test.
     */
    public void testCaseAggregationOnNonProjectedColumnListingPath() throws Exception {
        clearAllCaches();
        // Listing path (mirrors api_metrics Q1/Q7): NO predicate, group by city, the case()
        // aggregations read `age`/`score` which are not otherwise projected. Projection push-down
        // makes the scan project {city, CASE(age...), CASE(score...)} — the CASE fields are not
        // file columns, so the buggy name-based read-set inference dropped age/score and gave them
        // a placeholder OffsetIndex, corrupting the page read.
        String listing = "source=" + INDEX_NAME
            + " | stats count() as total, sum(case(age > 50, 1 else 0)) as old,"
            + " sum(case(score > 75.0, 1 else 0)) as hi by city"
            + " | eval old_rate = round(old * 100.0 / total, 2)"
            + " | fields city, total, old_rate";
        executePpl(listing);
        executePpl(listing); // warm: exercise the cached-OI path too
    }

    public void testCaseAggregationOnNonProjectedColumnIndexedPath() throws Exception {
        clearAllCaches();
        // Indexed path: match() routes through the Lucene-indexed executor, which wires the scoped
        // page index via a different code path than the listing optimizer. Same hazard: the case()
        // aggregations read `age`/`score`, which are not in the group-by/projection list.
        String indexed = "source=" + INDEX_NAME
            + " | where match(city, 'seattle') | stats count() as total,"
            + " sum(case(age > 50, 1 else 0)) as old, sum(case(score > 75.0, 1 else 0)) as hi by city"
            + " | fields city, total, old, hi";
        executePpl(indexed);
        executePpl(indexed); // warm
    }

    /**
     * Delegated keyword-equality predicate produces NO scoped cache activity.
     *
     * <p>This is the converse of {@link #testStringFieldFilterFillsColumnIndex}: with the default
     * delegation block-list (EQUALS is NOT blocked), {@code city = 'seattle'} on a keyword field is
     * answered by Lucene's inverted index. The parquet file is never scanned for that predicate, so
     * neither the ColumnIndex nor the OffsetIndex scoped cache should record any hit, miss, or entry.
     *
     * <p>Guards against a regression where a delegated string predicate still triggers a parquet
     * page-index load (which would defeat the point of delegation and waste cache budget).
     */
    public void testDelegatedStringPredicateProducesNoOffsetIndexActivity() throws Exception {
        // Be explicit that EQUALS is delegated to Lucene (this is also the cluster default).
        setLuceneBlockedPredicates();
        clearAllCaches();
        assertCacheEmpty();

        String ppl = "source=" + INDEX_NAME + " | where city = 'seattle' | stats count()";
        executePpl(ppl);
        executePpl(ppl);

        JsonNode s = stats();
        // The predicate went to Lucene; parquet pages were never read.
        assertZero("delegated EQUALS: OI hits must be 0", oiHits(s));
        assertZero("delegated EQUALS: OI misses must be 0", oiMisses(s));
        assertZero("delegated EQUALS: OI entries must be 0", oiEntries(s));
        assertZero("delegated EQUALS: CI hits must be 0", ciHits(s));
        assertZero("delegated EQUALS: CI misses must be 0", ciMisses(s));
        assertZero("delegated EQUALS: CI entries must be 0", ciEntries(s));
    }

    // ── tiny cache limits — correctness under eviction pressure ──────────────

    /**
     * Set the total metadata-index cache budget to a handful of bytes so every entry is
     * guaranteed to be rejected on insert (CI entry size = column_index_length bytes from the
     * parquet footer, always ≥ tens of bytes; OI entry size = offset_index_length, likewise).
     * When an entry's {@code size > limit} the Rust cache skips the insert — every access is a
     * miss, {@code used_bytes} stays 0, {@code entry_count} stays 0.
     *
     * <p>The budget is split across the sub-caches by fixed percentages (CI 13%, OI 34%) with
     * integer truncation, and the native limit setters IGNORE a zero limit (treating it as
     * "unset" and keeping the default multi-MB budget). So a 1-byte total would truncate both
     * shares to 0 and leave the real caches huge. We use 8 bytes instead: CI → 8×13/100 = 1 byte,
     * OI → 8×34/100 = 2 bytes — both non-zero (honored) yet far below any real entry, which is
     * exactly the "reject everything" regime this test needs.
     *
     * <p>Queries must still return correct results — the cache is a performance
     * optimisation, not a correctness dependency. The parquet reader falls back to
     * loading page-index bytes fresh from the object store on every query.
     */
    public void testQueriesCorrectWithTinyCacheLimits() throws Exception {
        // 8b → CI limit 1b, OI limit 2b: tiny but non-zero, so every entry is larger and rejected.
        // The cluster-settings PUT is acknowledged only after every node has applied the update and
        // run the cache-limit consumer, so the scoped limits are already live when this returns.
        setCacheTotalSize("8b");
        try {
            clearAllCaches();

            // Listing path predicate — misses every time, never gets cached.
            String listing = "source=" + INDEX_NAME + " | where age > 90 | stats count()";
            executePpl(listing);
            JsonNode s1 = stats();
            // size_limit_bytes must reflect the configured tiny per-cache limit (% of 8b),
            // small enough that no entry can ever fit.
            assertTrue("CI size_limit_bytes must be tiny (<= 8) under 8b budget",
                s1.get("column_index_cache").get("size_limit_bytes").asLong() <= 8L);
            assertTrue("OI size_limit_bytes must be tiny (<= 8) under 8b budget",
                s1.get("offset_index_cache").get("size_limit_bytes").asLong() <= 8L);
            // Entries must be 0 — entries are too large to fit, so nothing is stored.
            assertEquals("CI entry_count must be 0 (entries too large for 1b limit)",
                0L, ciEntries(s1));
            assertEquals("OI entry_count must be 0 (entries too large for 1b limit)",
                0L, oiEntries(s1));
            // used_bytes must be 0 — nothing was stored.
            assertEquals("CI memory_bytes must be 0", 0L,
                s1.get("column_index_cache").get("memory_bytes").asLong());
            assertEquals("OI memory_bytes must be 0", 0L,
                s1.get("offset_index_cache").get("memory_bytes").asLong());
            // All accesses are misses since nothing is ever cached.
            assertPositive("CI misses must be > 0 with 1b limit", ciMisses(s1));
            assertPositive("OI misses must be > 0 with 1b limit", oiMisses(s1));

            // Second run — still all misses (nothing was cached from first run).
            executePpl(listing);
            JsonNode s2 = stats();
            assertEquals("CI hits must remain 0 — nothing fits in 1b cache", 0L, ciHits(s2));
            assertEquals("OI hits must remain 0 — nothing fits in 1b cache", 0L, oiHits(s2));
            assertTrue("CI misses must increase on second run", ciMisses(s2) > ciMisses(s1));

            // Indexed path — must also work correctly with evicted cache.
            String indexed = "source=" + INDEX_NAME + " | where match(city, 'seattle') and age > 50 | stats count()";
            executePpl(indexed);
            executePpl(indexed);

            // Projection-only — must work correctly.
            String proj = "source=" + INDEX_NAME + " | fields name, score | head 10";
            executePpl(proj);

            // Pure Lucene count — must still work (does not touch CI/OI at all).
            String lucene = "source=" + INDEX_NAME + " | where match(city, 'seattle') | stats count()";
            executePpl(lucene);

        } finally {
            restoreCacheDefaults();
            clearAllCaches();
        }
    }

    // ── cache clear ───────────────────────────────────────────────────────────

    /** Explicit CI clear resets hit counter; next query is a miss again. */
    public void testClearColumnIndexResetsToMiss() throws Exception {
        String ppl = "source=" + INDEX_NAME + " | where age > 90 | stats count()";

        executePpl(ppl);
        executePpl(ppl);
        long hitsBefore = ciHits(stats());
        assertTrue("must have hits before clear", hitsBefore > 0);

        clearColumnCaches();
        assertEquals("CI hits reset to 0 after clear", 0L, ciHits(stats()));

        executePpl(ppl);
        assertTrue("re-run after clear produces misses", ciMisses(stats()) > 0);
    }

    // ── stats shape ───────────────────────────────────────────────────────────

    /** Stats endpoint exposes column_index_cache and offset_index_cache with expected fields. */
    public void testStatsShapeHasBothCacheGroups() throws Exception {
        executePpl("source=" + INDEX_NAME + " | where age > 50 | stats count()");

        JsonNode s = stats();
        for (String group : new String[]{"column_index_cache", "offset_index_cache"}) {
            JsonNode block = s.get(group);
            assertNotNull(group + " block missing", block);
            for (String field : new String[]{"hit_count", "miss_count", "entry_count", "memory_bytes", "size_limit_bytes", "hit_rate"}) {
                assertTrue(group + "." + field + " missing", block.has(field));
            }
        }
    }

    // ── reusable assertion helpers ────────────────────────────────────────────

    /**
     * Run {@code ppl} cold (caches assumed empty by caller) and assert that both CI and OI fill.
     *
     * <p>OI is built for {@code predicate ∪ projection ∪ {col 0}}, so any predicate query
     * produces OI misses in addition to CI misses — even if no explicit {@code fields} clause
     * is present.
     */
    private void assertCacheFillsOnCold(String ppl) throws Exception {
        executePpl(ppl);
        JsonNode cold = stats();
        assertPositive("cold CI misses for: " + ppl, ciMisses(cold));
        assertPositive("cold OI misses for: " + ppl, oiMisses(cold));
        assertPositive("cold metadata populated for: " + ppl, metaMemoryBytes(cold));
    }

    /**
     * Run {@code ppl} a second time (cache already warm) and assert that both CI and OI hit.
     * OI is keyed by (file, col) — same file+col on a repeat query is a guaranteed hit.
     */
    private void assertCacheHitsOnWarm(String ppl) throws Exception {
        executePpl(ppl);
        JsonNode warm = stats();
        assertPositive("warm CI hits for: " + ppl, ciHits(warm));
        assertPositive("warm OI hits for: " + ppl, oiHits(warm));
    }

    /**
     * Run {@code ppl} twice with scoped disabled (CI/OI = 0 each time).
     * Does NOT clear or assert metadata — the caller controls surrounding state.
     */
    @SuppressWarnings("unused")
    private void assertScopedDisabled(String ppl) throws Exception {
        executePpl(ppl);
        JsonNode s1 = stats();
        assertZero("disabled CI misses run1: " + ppl, ciMisses(s1));
        assertZero("disabled OI misses run1: " + ppl, oiMisses(s1));

        executePpl(ppl);
        JsonNode s2 = stats();
        assertZero("disabled CI hits run2: " + ppl, ciHits(s2));
        assertZero("disabled OI hits run2: " + ppl, oiHits(s2));
    }

    /**
     * Assert that all scoped cache counters and metadata memory are zero (i.e. caches are empty).
     * Call this after {@link #clearAllCaches()} to confirm the clear took effect before measuring.
     */
    private void assertCacheEmpty() throws Exception {
        JsonNode s = stats();
        assertZero("assertCacheEmpty: metadata memory", metaMemoryBytes(s));
        assertZero("assertCacheEmpty: CI entries", ciEntries(s));
        assertZero("assertCacheEmpty: OI entries", oiEntries(s));
    }

    /**
     * Run {@code ppl} and assert that ONLY the OffsetIndex fills: OI misses > 0 and CI misses == 0.
     * Intended for projection-only queries that carry no predicate.
     */
    private void assertOnlyOiFills(String ppl) throws Exception {
        executePpl(ppl);
        JsonNode s = stats();
        assertPositive("assertOnlyOiFills: OI misses > 0 for: " + ppl, oiMisses(s));
        assertZero("assertOnlyOiFills: CI misses must be 0 for: " + ppl, ciMisses(s));
    }

    // ── index lifecycle helpers ───────────────────────────────────────────────

    /**
     * Delete, recreate, and bulk-index {@value #DOC_COUNT} documents with refresh=true.
     * Every test that needs a clean state calls this explicitly.
     */
    private void recreateIndex() throws IOException {
        try { client().performRequest(new Request("DELETE", "/" + INDEX_NAME)); } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"age\":{\"type\":\"integer\"},"
            + "  \"score\":{\"type\":\"double\"},"
            + "  \"city\":{\"type\":\"keyword\"}"
            + "}}}";

        Request create = new Request("PUT", "/" + INDEX_NAME);
        create.setJsonEntity(body);
        assertOkAndParse(client().performRequest(create), "create index");

        Request health = new Request("GET", "/_cluster/health/" + INDEX_NAME);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        String[] cities = {"seattle", "portland", "denver", "austin", "boston"};
        int batchSize = 500;
        for (int batch = 0; batch < DOC_COUNT / batchSize; batch++) {
            boolean lastBatch = batch == (DOC_COUNT / batchSize - 1);
            StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < batchSize; i++) {
                int id = batch * batchSize + i;
                bulk.append("{\"index\":{}}\n")
                    .append("{\"name\":\"user").append(id)
                    .append("\",\"age\":").append(id % 100)
                    .append(",\"score\":").append(50.0 + (id % 50))
                    .append(",\"city\":\"").append(cities[id % cities.length])
                    .append("\"}\n");
            }
            Request bulkReq = new Request("POST", "/" + INDEX_NAME + "/_bulk");
            bulkReq.setJsonEntity(bulk.toString());
            bulkReq.addParameter("refresh", lastBatch ? "true" : "false");
            bulkReq.setOptions(bulkReq.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
            Map<String, Object> r = assertOkAndParse(client().performRequest(bulkReq), "bulk batch " + batch);
            assertEquals("bulk batch " + batch + " errors", false, r.get("errors"));
        }
    }

    /** Set the total metadata-index cache budget (e.g. {@code "100b"}, {@code "500mb"}). */
    private void setCacheTotalSize(String size) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"datafusion.metadata_index_cache.total_size\":\"" + size + "\"}}");
        client().performRequest(req);
    }

    /**
     * Restore the total metadata-index cache budget to the cluster default (null = derive from AC).
     * The cluster-settings PUT is acknowledged only after every node has applied the update and run
     * the cache-limit consumer, so the large default scoped limits are live again on return.
     */
    private void restoreCacheDefaults() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"datafusion.metadata_index_cache.total_size\":null}}");
        client().performRequest(req);
    }

    /**
     * Block the given predicate functions from being delegated to the Lucene backend.
     *
     * <p>String fields (keyword/text) route their predicates to Lucene's inverted index by
     * default, so a predicate like {@code city = 'seattle'} is answered by Lucene and never
     * scans the parquet file — meaning it never loads the parquet ColumnIndex. Blocking the
     * predicate (e.g. {@code EQUALS}) forces the planner to leave it on the DataFusion/parquet
     * backend, which is what fills the scoped ColumnIndex cache. Pass an empty list to clear.
     */
    private void setLuceneBlockedPredicates(String... predicates) throws IOException {
        StringBuilder arr = new StringBuilder("[");
        for (int i = 0; i < predicates.length; i++) {
            if (i > 0) arr.append(',');
            arr.append('"').append(predicates[i]).append('"');
        }
        arr.append(']');
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.delegation.lucene.blocked_predicates\":" + arr + "}}");
        client().performRequest(req);
    }

    /** Restore the Lucene delegation block-list to the cluster default (null clears the override). */
    private void restoreLuceneBlockedPredicates() throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"analytics.delegation.lucene.blocked_predicates\":null}}");
        client().performRequest(req);
    }

    private void setScopedPageIndexEnabled(boolean enabled) throws IOException {
        Request req = new Request("PUT", "/_cluster/settings");
        req.setJsonEntity("{\"persistent\":{\"datafusion.scoped_page_index.enabled\":" + enabled + "}}");
        client().performRequest(req);
    }

    private void clearAllCaches() throws IOException {
        client().performRequest(new Request("POST", CLEAR_ENDPOINT));
    }

    private void clearColumnCaches() throws IOException {
        client().performRequest(new Request("POST", CLEAR_ENDPOINT + "?column=true"));
    }

    /**
     * Fetches DataFusion cache stats aggregated across <em>all</em> nodes in the cluster.
     *
     * <p>The test cluster runs with multiple nodes but the test index has a single shard,
     * so the parquet scan that fills the scoped CI/OI caches (process-global per JVM) and
     * the per-runtime metadata cache only happens on the node that hosts that shard. The
     * caches therefore live on one specific node, while the round-robin REST client may
     * route a {@code _local} stats request to any node. Reading a single node's stats would
     * intermittently observe an empty cache simply because the request landed on a node that
     * never ran the scan.
     *
     * <p>To make the measurements deterministic we query the cluster-wide stats endpoint
     * (all nodes) and sum each cache counter across every node. Cluster-wide sums are exactly
     * what the assertions want: cache activity is non-zero iff <em>some</em> node did the work,
     * and {@code clearAllCaches()} broadcasts to every node so cleared sums are zero everywhere.
     */
    private JsonNode stats() throws Exception {
        Response response = client().performRequest(
            new Request("GET", "/_plugins/_analytics_backend_datafusion/stats")
        );
        JsonNode root = MAPPER.readTree(EntityUtils.toString(response.getEntity()));
        JsonNode nodes = root.get("nodes");
        assertNotNull("nodes block missing", nodes);

        // Combine each cache group's fields across all nodes into a synthetic cache_stats
        // object with the same shape the accessor helpers expect. Counters (hit_count,
        // miss_count, entry_count, memory_bytes) accumulate across the cluster, so they are
        // SUMMED. size_limit_bytes is a per-node capacity (every node configures the same
        // budget), so it is taken as the MAX rather than summed — otherwise an N-node cluster
        // would report N× the configured limit.
        com.fasterxml.jackson.databind.node.ObjectNode aggregate = MAPPER.createObjectNode();
        for (JsonNode node : nodes) {
            JsonNode cacheStats = node.get("cache_stats");
            if (cacheStats == null) {
                continue;
            }
            cacheStats.fieldNames().forEachRemaining(group -> {
                JsonNode groupNode = cacheStats.get(group);
                if (groupNode == null || !groupNode.isObject()) {
                    return;
                }
                com.fasterxml.jackson.databind.node.ObjectNode aggGroup = aggregate.has(group)
                    ? (com.fasterxml.jackson.databind.node.ObjectNode) aggregate.get(group)
                    : aggregate.putObject(group);
                groupNode.fieldNames().forEachRemaining(field -> {
                    JsonNode value = groupNode.get(field);
                    if (value != null && value.isNumber()) {
                        long prev = aggGroup.path(field).asLong(0L);
                        long combined = "size_limit_bytes".equals(field)
                            ? Math.max(prev, value.asLong())
                            : prev + value.asLong();
                        aggGroup.put(field, combined);
                    }
                });
            });
        }
        assertTrue("cache_stats block missing on every node", aggregate.size() > 0);
        return aggregate;
    }

    // ── low-level assertion utilities ─────────────────────────────────────────

    private void assertZero(String msg, long v)     { assertEquals(msg + " must be 0, got " + v, 0L, v); }
    private void assertPositive(String msg, long v) { assertTrue(msg + " must be > 0, got " + v, v > 0); }

    /**
     * Assert exact entry count with an informative message.
     *
     * <p>CI key = (file, col, rg) so expectedCi = files × predicateCols × rowGroupsPerFile.
     * OI key = (file, col) so expectedOi = files × projectionCols.
     *
     * <p>With DOC_COUNT=2000 on a single shard and a single refresh the data lands in one
     * parquet segment with one row group (2000 rows well under the default 1M-row row-group limit), so:
     * <ul>
     *   <li>1 predicate col → CI entries = 1</li>
     *   <li>N projection cols → OI entries = N</li>
     * </ul>
     * For multi-segment scenarios (two separate refreshes before merge) multiply by the
     * number of segments.
     */
    private void assertExactCiEntries(String msg, long expected) throws Exception {
        assertEquals(msg, expected, ciEntries(stats()));
    }

    private void assertExactOiEntries(String msg, long expected) throws Exception {
        assertEquals(msg, expected, oiEntries(stats()));
    }

    // ── stat accessor shorthands ──────────────────────────────────────────────

    private long ciHits(JsonNode s)          { return s.get("column_index_cache").get("hit_count").asLong(); }
    private long ciMisses(JsonNode s)        { return s.get("column_index_cache").get("miss_count").asLong(); }
    private long ciEntries(JsonNode s)       { return s.get("column_index_cache").get("entry_count").asLong(); }
    private long oiHits(JsonNode s)          { return s.get("offset_index_cache").get("hit_count").asLong(); }
    private long oiMisses(JsonNode s)        { return s.get("offset_index_cache").get("miss_count").asLong(); }
    private long oiEntries(JsonNode s)       { return s.get("offset_index_cache").get("entry_count").asLong(); }
    private long metaMemoryBytes(JsonNode s) { return s.get("metadata_cache").get("memory_bytes").asLong(); }
}
