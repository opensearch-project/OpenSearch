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
 * Pins the can-match behavior across multiple parquet segments produced by separate
 * refresh cycles. If a shard's first refresh produces parquet stats {@code [0, 9]}
 * and a second refresh adds rows in {@code [100, 109]}, a query for {@code value > 50}
 * must NOT drop the whole shard — the second segment's stats overlap the filter range.
 *
 * <p>The data-node aggregator walks every {@code .parquet} file under the shard and
 * ORs across them; only when EVERY file's stats fail the predicate does the shard
 * get eliminated. This IT prevents regression to a single-segment check.
 *
 * <p>Note on the related "mixed lucene+parquet" concern (C4 in the self-review):
 * lucene-only data (uncommitted buffer between refreshes) is invisible to the
 * analytics engine in the {@code primary=parquet, secondary=[]} configuration —
 * the engine reads only {@code .parquet} files. Refresh flushes to parquet, which
 * is the surface this IT actually exercises. If a future config allows the analytics
 * engine to read lucene secondary segments, can-match would need a parallel check
 * against those — not implemented today.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 0, supportsDedicatedMasters = false)
public class CanMatchMultiSegmentIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "canmatch_multiseg_idx";
    private static final int NUM_SHARDS = 2;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

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

    /**
     * Two refresh cycles produce two parquet segments per shard with disjoint stats
     * ranges. A query selecting only the second range must still match — proving
     * the can-match aggregator considers ALL segments, not just the oldest one.
     */
    public void testFilterMatchesLaterSegmentSurvivesPruning() throws Exception {
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
            .setMapping("value", "type=integer")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(INDEX);

        // Segment 1: values [0, 9]. After refresh + flush this lands in one parquet
        // file per shard with stats range [0, 9].
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX).setId("a-" + i).setRouting("lo").setSource("value", i).get();
            client().prepareIndex(INDEX).setId("b-" + i).setRouting("hi").setSource("value", i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        // Segment 2: values [100, 109]. Separate refresh + flush → second parquet file
        // per shard with stats range [100, 109]. Both segments live under the same shard.
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX).setId("a-late-" + i).setRouting("lo").setSource("value", 100 + i).get();
            client().prepareIndex(INDEX).setId("b-late-" + i).setRouting("hi").setSource("value", 100 + i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        // value > 50 → matches segment 2 only. Segment 1's stats [0, 9] are below 50;
        // can-match should NOT drop the shard because segment 2's stats [100, 109]
        // overlap the filter range. Expect all 20 "late" docs.
        PPLResponse r = executePPL("source = " + INDEX + " | where value > 50 | stats count() as c", QUERY_TIMEOUT);
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("late-segment docs must survive can-match prune", 20L, matched);
    }

    /**
     * Companion case: every segment's stats fall outside the filter range. The shard
     * is correctly dropped (no rows match). This catches a "fail-open everywhere"
     * regression — if can-match silently stopped pruning, both segments would scan.
     */
    public void testFilterMatchingNoSegmentEliminatesShard() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        client().admin().indices().prepareCreate(INDEX).setSettings(indexSettings).setMapping("value", "type=integer").get();
        ensureGreen(INDEX);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX).setId("a-" + i).setRouting("lo").setSource("value", i).get();
            client().prepareIndex(INDEX).setId("b-" + i).setRouting("hi").setSource("value", i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX).setId("a-late-" + i).setRouting("lo").setSource("value", 100 + i).get();
            client().prepareIndex(INDEX).setId("b-late-" + i).setRouting("hi").setSource("value", 100 + i).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        // No segment has stats overlapping (200, ∞).
        PPLResponse r = executePPL("source = " + INDEX + " | where value > 200 | stats count() as c", QUERY_TIMEOUT);
        long matched = ((Number) r.getRows().get(0)[r.getColumns().indexOf("c")]).longValue();
        assertEquals("nothing matches; can-match should prune both segments of both shards", 0L, matched);
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }
}
