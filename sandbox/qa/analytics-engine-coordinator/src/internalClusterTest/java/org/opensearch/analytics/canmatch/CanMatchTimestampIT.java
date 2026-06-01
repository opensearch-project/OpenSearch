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
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end can-match coverage for {@code @timestamp} (date-typed) filters — the most
 * common real-world predicate. The extractor has a dedicated TIMESTAMP/DATE branch in
 * {@code extractLongValue} that pulls epoch millis; this IT pins that path through the
 * full coordinator → data-node → parquet-stats flow.
 *
 * <p>Two shards seeded with disjoint time windows:
 * <ul>
 *   <li>shard A: rows in January 2024 ({@code 1_704_067_200_000} .. {@code 1_704_240_000_000})</li>
 *   <li>shard B: rows in March 2024 ({@code 1_709_251_200_000} .. {@code 1_709_424_000_000})</li>
 * </ul>
 * Queries with windows that overlap only one range exercise pruning; queries spanning
 * both windows pin the fail-open path.
 *
 * <p>If PPL doesn't surface the date literal as a {@link org.apache.calcite.rex.RexLiteral}
 * the extractor recognises (TIMESTAMP family or numeric epoch), no filter is extracted and
 * the test still passes — counts are correct, pruning is silently a no-op. That outcome is
 * itself a finding worth surfacing.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class CanMatchTimestampIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "canmatch_ts_idx";
    private static final int NUM_SHARDS = 2;
    private static final int DOCS_PER_RANGE = 10;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    // Disjoint time windows, one second apart, ten docs each.
    private static final long EARLY_BASE_MS = 1_704_067_200_000L; // 2024-01-01T00:00:00Z
    private static final long LATE_BASE_MS = 1_709_251_200_000L;  // 2024-03-01T00:00:00Z

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
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
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
            .build();
    }

    /** Filter selects only the late-window shard. */
    public void testTimestampGreaterThanSelectsLateShard() throws Exception {
        createAndSeedIndex();
        // Cutoff between the two windows. Any timestamp on/after 2024-02-01 only matches shard B.
        PPLResponse r = executePPL(
            "source = " + INDEX + " | where @timestamp >= timestamp('2024-02-01 00:00:00') | stats count() as c",
            QUERY_TIMEOUT
        );
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("only late-range docs match", DOCS_PER_RANGE, matched);
    }

    /** Filter selects only the early-window shard. */
    public void testTimestampLessThanSelectsEarlyShard() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL(
            "source = " + INDEX + " | where @timestamp < timestamp('2024-02-01 00:00:00') | stats count() as c",
            QUERY_TIMEOUT
        );
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("only early-range docs match", DOCS_PER_RANGE, matched);
    }

    /** Filter excludes both windows — can-match should prune both shards. */
    public void testTimestampOutsideBothWindowsEliminatesAllShards() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL(
            "source = " + INDEX + " | where @timestamp >= timestamp('2025-01-01 00:00:00') | stats count() as c",
            QUERY_TIMEOUT
        );
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("no docs after 2025", 0L, matched);
    }

    /** Filter spans both windows — pruning must skip neither shard. */
    public void testTimestampSpanningBothWindowsKeepsBothShards() throws Exception {
        createAndSeedIndex();
        PPLResponse r = executePPL(
            "source = " + INDEX + " | where @timestamp >= timestamp('2024-01-01 00:00:00') | stats count() as c",
            QUERY_TIMEOUT
        );
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("both shards contribute", (long) DOCS_PER_RANGE * 2, matched);
    }

    // ── fixture ────────────────────────────────────────────────────────────

    private void createAndSeedIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("@timestamp", "type=date,format=epoch_millis||strict_date_optional_time")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(INDEX);

        // Force-route so early values land on one shard and late values on the other.
        for (int i = 0; i < DOCS_PER_RANGE; i++) {
            client().prepareIndex(INDEX)
                .setId("early-" + i)
                .setRouting("lo")
                .setSource("@timestamp", EARLY_BASE_MS + (long) i * 1_000L)
                .get();
            client().prepareIndex(INDEX)
                .setId("late-" + i)
                .setRouting("hi")
                .setSource("@timestamp", LATE_BASE_MS + (long) i * 1_000L)
                .get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }
}
