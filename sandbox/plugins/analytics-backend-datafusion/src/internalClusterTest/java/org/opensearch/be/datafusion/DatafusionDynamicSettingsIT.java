/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class DatafusionDynamicSettingsIT extends OpenSearchIntegTestCase {

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
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    public void testBatchSizeCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.batch_size", 16384).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("16384", response.getTransientSettings().get("datafusion.indexed.batch_size"));
    }

    public void testParquetPushdownFiltersCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.parquet_pushdown_filters", true).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("true", response.getTransientSettings().get("datafusion.indexed.parquet_pushdown_filters"));
    }

    public void testMinSkipRunDefaultCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.min_skip_run_default", 2048).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("2048", response.getTransientSettings().get("datafusion.indexed.min_skip_run_default"));
    }

    public void testSelectivityThresholdCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 0.5).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("0.5", response.getTransientSettings().get("datafusion.indexed.min_skip_run_selectivity_threshold"));
    }

    public void testCostPredicateCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.cost_predicate", 5).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("5", response.getTransientSettings().get("datafusion.indexed.cost_predicate"));
    }

    public void testCostCollectorCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.cost_collector", 25).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("25", response.getTransientSettings().get("datafusion.indexed.cost_collector"));
    }

    public void testMaxCollectorParallelismCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.max_collector_parallelism", 4).build())
            .get();
        assertTrue(response.isAcknowledged());
        assertEquals("4", response.getTransientSettings().get("datafusion.indexed.max_collector_parallelism"));
    }

    public void testInvalidBatchSizeIsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.indexed.batch_size", 0).build())
                .get()
        );
    }

    public void testInvalidSelectivityThresholdIsRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 1.5).build())
                .get()
        );
    }

    public void testMultipleSettingsCanBeUpdatedAtOnce() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("datafusion.indexed.batch_size", 4096)
                    .put("datafusion.indexed.cost_collector", 50)
                    .put("datafusion.indexed.max_collector_parallelism", 2)
                    .build()
            )
            .get();
        assertTrue(response.isAcknowledged());

        Settings transient_ = response.getTransientSettings();
        assertEquals("4096", transient_.get("datafusion.indexed.batch_size"));
        assertEquals("50", transient_.get("datafusion.indexed.cost_collector"));
        assertEquals("2", transient_.get("datafusion.indexed.max_collector_parallelism"));
    }

    public void testSettingsCanBeResetToDefault() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("datafusion.indexed.batch_size", 4096).build())
            .get();

        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().putNull("datafusion.indexed.batch_size").build())
            .get();
        assertTrue(response.isAcknowledged());
    }
}
