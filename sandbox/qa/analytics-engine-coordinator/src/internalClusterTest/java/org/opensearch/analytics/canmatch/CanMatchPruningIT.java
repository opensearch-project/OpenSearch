/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.canmatch;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.settings.AnalyticsQuerySettings;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration test for the can-match pre-filter phase, modelling the flagship time-series / log
 * analytics scenario: a {@code logs} stream rolls over into one dated index per day, and a
 * dashboard issues a {@code range(@timestamp)} query — "last N days", "since T", "before T", or a
 * bounded window — sorted by time. Can-match inspects each shard's parquet row-group min/max for
 * {@code @timestamp} and prunes the days whose range is provably disjoint from the query window, so
 * those shards never run a fragment. On a real deployment this is the difference between waking
 * hundreds of cold daily shards and touching only the handful the window overlaps.
 *
 * <p>The fixture is a week of daily indices ({@value #DAY_INDEX_PREFIX}{@code 08 .. 14}), each with
 * a single shard so day ↔ shard is 1:1 and every doc in an index falls entirely within that day.
 * Pruning is therefore deterministic: a day is kept iff its date range overlaps the query window,
 * independent of routing. Each test asserts both the fragment-dispatch count (= surviving days) and
 * the returned row count (= surviving days × {@value #DOCS_PER_DAY}). The cases span the matrix:
 * <ul>
 *   <li><b>No filter / covering window</b> — nothing disjoint → every day runs. Guards over-pruning.</li>
 *   <li><b>Window beyond all data</b> — every day disjoint → all pruned, but can-match force-keeps
 *       one shard so the query still yields a valid empty result (mirrors vanilla's all-pruned
 *       special case). The drop from {@value #TOTAL_DAYS} to 1 is direct proof pruning happened.</li>
 *   <li><b>Trailing / lower-bound window</b> ("last N days") — older days prune, recent survive.</li>
 *   <li><b>Upper-bound window</b> ("before T") — newer days prune, older survive.</li>
 *   <li><b>Bounded window</b> (the dashboard "last 24h" shape) — days on both sides of the window
 *       prune; only days inside survive.</li>
 * </ul>
 *
 * <p>Fragment dispatches are counted by intercepting {@link FragmentExecutionAction} on each data
 * node with a {@link MockTransportService} — fragment execution runs only on shards that survived
 * can-match, so the count equals the surviving-shard (= surviving-day) count.
 *
 * <p>Indices are addressed as a PPL comma-list ({@code source = a,b,c}), a native multi-index union
 * scan that routes through the distributed analytics engine where can-match runs. A {@code logs-*}
 * wildcard would instead take the legacy push-down path and bypass can-match, and SQL {@code FROM
 * a,b,c} is a cross-join — so the PPL comma-list is the faithful way to drive this path.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class CanMatchPruningIT extends OpenSearchIntegTestCase {

    // A week of daily rollover: logs-parquet-2026-07-08 .. logs-parquet-2026-07-14, one shard each.
    private static final String DAY_INDEX_PREFIX = "logs-parquet-2026-07-";
    private static final int TOTAL_DAYS = 7;
    private static final int FIRST_DAY = 8;   // 2026-07-08
    private static final int LAST_DAY = FIRST_DAY + TOTAL_DAYS - 1; // 2026-07-14
    private static final int DOCS_PER_DAY = 5;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            // These cases prune on filters alone, and a week of daily indices is far below the
            // production trigger, so force the phase on. What is under test is the pruning itself,
            // not the fan-out heuristic that decides when to pay for it — that lives in
            // CanMatchTriggerTests.
            .put(AnalyticsQuerySettings.PRE_FILTER_SHARD_SIZE.getKey(), 1)
            .build();
    }

    /**
     * No range predicate → nothing is extractable, so can-match is skipped and every day's shard
     * runs. Guards against the extractor over-reaching and pruning when there is no window.
     */
    public void testNoDateFilterScansEveryDay() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL("source = " + indices + " | fields host");
            assertEquals("no window → every day runs", TOTAL_DAYS, dispatches.get());
            assertEquals("all docs returned", TOTAL_DAYS * DOCS_PER_DAY, response.getRows().size());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * A bounded window that spans the whole week ({@code 2026-07-01 .. 2026-07-31}) — every day
     * overlaps, so none is pruned and all run. Brackets the prune-everything case: can-match prunes
     * exactly when a day is disjoint from the window, never otherwise.
     */
    public void testWindowCoveringEveryDayScansEveryDay() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL(
                "source = " + indices + " | where `@timestamp` >= '2026-07-01 00:00:00' and `@timestamp` <= '2026-07-31 23:59:59' | fields host"
            );
            assertEquals("window covers every day → nothing pruned", TOTAL_DAYS, dispatches.get());
            assertEquals("all docs returned", TOTAL_DAYS * DOCS_PER_DAY, response.getRows().size());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * A window entirely after all data ({@code @timestamp >= '2026-08-01'}) — every day is provably
     * disjoint. Can-match prunes all but one: it force-keeps a single shard so the query still
     * produces a valid (empty) result envelope, mirroring vanilla's all-pruned special case. So
     * exactly 1 fragment dispatches (down from {@link #TOTAL_DAYS}) and the result is empty — the
     * drop is the proof pruning happened; a broken build dispatches to every day.
     */
    public void testWindowBeyondAllDaysPrunesToSingleShard() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL(
                "source = " + indices + " | where `@timestamp` >= '2026-08-01 00:00:00' | fields host"
            );
            assertEquals("all days pruned; one kept for a valid empty result", 1, dispatches.get());
            assertTrue("no rows should match a window past all data", response.getRows().isEmpty());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * A trailing "last 2 days" window ({@code @timestamp >= '2026-07-13'}) — the classic dashboard
     * lower-bound query. Days 08–12 lie below the cutoff and prune; days 13 and 14 survive. Exactly
     * 2 fragments dispatch and only those two days' docs come back.
     */
    public void testTrailingWindowKeepsRecentDays() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL(
                "source = " + indices + " | where `@timestamp` >= '2026-07-13 00:00:00' | fields host"
            );
            assertEquals("only days 13–14 survive the trailing window", 2, dispatches.get());
            assertEquals("only the two surviving days' docs return", 2 * DOCS_PER_DAY, response.getRows().size());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * An upper-bound "before" window ({@code @timestamp <= '2026-07-09 23:59:59'}) — days 10–14 lie
     * above the cutoff and prune; days 08 and 09 survive. Mirror image of the trailing window.
     */
    public void testUpperBoundWindowKeepsOlderDays() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL(
                "source = " + indices + " | where `@timestamp` <= '2026-07-09 23:59:59' | fields host"
            );
            assertEquals("only days 08–09 survive the upper-bound window", 2, dispatches.get());
            assertEquals("only the two surviving days' docs return", 2 * DOCS_PER_DAY, response.getRows().size());
        } finally {
            clearTransportRules();
        }
    }

    /**
     * A bounded window ({@code 2026-07-10 .. 2026-07-11}) — the flagship "last 24h" dashboard shape,
     * where days on <em>both</em> sides of the window must prune. Days 08–09 (below) and 12–14
     * (above) prune; only days 10 and 11 survive. Proves can-match applies both bounds at once, not
     * just a one-sided cutoff.
     */
    public void testBoundedWindowKeepsOnlyDaysInRange() throws Exception {
        String indices = createDailyIndices();

        AtomicInteger dispatches = countFragmentDispatches();
        try {
            PPLResponse response = executePPL(
                "source = " + indices + " | where `@timestamp` >= '2026-07-10 00:00:00' and `@timestamp` <= '2026-07-11 23:59:59' | fields host"
            );
            assertEquals("only days 10–11 fall inside the bounded window", 2, dispatches.get());
            assertEquals("only the two surviving days' docs return", 2 * DOCS_PER_DAY, response.getRows().size());
        } finally {
            clearTransportRules();
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Installs a counting behavior for {@link FragmentExecutionAction} on every data node. */
    private AtomicInteger countFragmentDispatches() {
        AtomicInteger counter = new AtomicInteger();
        for (String node : internalCluster().getDataNodeNames()) {
            MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, node);
            mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
                counter.incrementAndGet();
                handler.messageReceived(request, channel, task);
            });
        }
        return counter;
    }

    private void clearTransportRules() {
        for (String node : internalCluster().getDataNodeNames()) {
            ((MockTransportService) internalCluster().getInstance(TransportService.class, node)).clearAllRules();
        }
    }

    /**
     * Creates {@link #TOTAL_DAYS} single-shard parquet indices, one per day
     * ({@value #DAY_INDEX_PREFIX}{@code 08 .. 14}), each seeded with {@link #DOCS_PER_DAY} docs whose
     * {@code @timestamp} falls entirely within that day. Returns the comma-separated index list for
     * a multi-index PPL {@code source =} clause.
     */
    private String createDailyIndices() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        StringBuilder commaList = new StringBuilder();
        for (int day = FIRST_DAY; day <= LAST_DAY; day++) {
            String index = String.format(Locale.ROOT, "%s%02d", DAY_INDEX_PREFIX, day);

            CreateIndexResponse response = client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                // host is index=false so a keyword field doesn't require the FULL_TEXT_SEARCH (lucene)
                // secondary format — parquet columnar storage covers it on its own.
                .setMapping("@timestamp", "type=date", "host", "type=keyword,index=false")
                .get();
            assertTrue("index creation must be acknowledged", response.isAcknowledged());

            for (int i = 0; i < DOCS_PER_DAY; i++) {
                client().prepareIndex(index)
                    .setSource(
                        "@timestamp",
                        String.format(Locale.ROOT, "2026-07-%02dT%02d:00:00Z", day, i),
                        "host",
                        "host-" + day + "-" + i
                    )
                    .get();
            }
            client().admin().indices().prepareRefresh(index).get();
            client().admin().indices().prepareFlush(index).get();

            if (day > FIRST_DAY) {
                commaList.append(',');
            }
            commaList.append(index);
        }
        ensureGreen();
        // Wait until every shard has its documents committed and visible before querying.
        for (int day = FIRST_DAY; day <= LAST_DAY; day++) {
            String index = String.format(Locale.ROOT, "%s%02d", DAY_INDEX_PREFIX, day);
            assertBusy(() -> {
                PPLResponse response = executePPL("source = " + index + " | fields host");
                assertEquals("index " + index + " must have " + DOCS_PER_DAY + " visible docs", DOCS_PER_DAY, response.getRows().size());
            }, 30, TimeUnit.SECONDS);
        }
        return commaList.toString();
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
