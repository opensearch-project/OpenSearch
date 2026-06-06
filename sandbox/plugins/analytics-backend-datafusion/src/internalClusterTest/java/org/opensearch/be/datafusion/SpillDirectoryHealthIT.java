/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsActionType;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodeResponse;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesRequest;
import org.opensearch.be.datafusion.action.stats.DataFusionStatsNodesResponse;
import org.opensearch.be.datafusion.stats.SpillStats;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Integration test verifying that {@code datafusion.spill_directory} writability
 * is surfaced through the DataFusion stats endpoint when spill is enabled.
 *
 * <p>Boot-probe failure (missing or non-writable spill_directory at startup) is
 * exercised by unit tests in {@code DataFusionPluginBootProbeTests}; this IT only
 * covers the happy-path case where the cluster starts cleanly and the stats
 * endpoint reflects {@code directory_writable: true}.
 *
 * <p>The runtime probe runs at a 60s cadence — it is not exercised here to keep
 * test wall-clock under control. See {@code SpillDirectoryHealthMonitorTests} for
 * unit-level coverage of probe transitions.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class SpillDirectoryHealthIT extends OpenSearchIntegTestCase {

    private Path spillDir;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
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
        // nodeSettings(int) is invoked multiple times during cluster setup; allocate the
        // tempdir once so all invocations resolve to the same path.
        if (spillDir == null) {
            spillDir = createTempDir();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put("datafusion.spill_directory", spillDir.toString())
            .build();
    }

    public void testStatsReportsWritableDirectoryAtBoot() throws Exception {
        SpillStats spill = fetchSpillStats();
        assertEquals(spillDir.toString(), spill.getDirectory());
        assertTrue("spill_directory must be writable on a freshly-created tempdir", spill.isDirectoryWritable());
    }

    private SpillStats fetchSpillStats() {
        DataFusionStatsNodesResponse resp = client().execute(
            DataFusionStatsActionType.INSTANCE,
            new DataFusionStatsNodesRequest(new String[0], Set.of("spill"))
        ).actionGet();
        assertFalse("expected at least one node response", resp.getNodes().isEmpty());
        DataFusionStatsNodeResponse first = resp.getNodes().get(0);
        assertNotNull("DataFusionStats must be present on the node response", first.getStats());
        SpillStats spill = first.getStats().getSpillStats();
        assertNotNull("SpillStats section must be populated when spill is enabled", spill);
        return spill;
    }
}
