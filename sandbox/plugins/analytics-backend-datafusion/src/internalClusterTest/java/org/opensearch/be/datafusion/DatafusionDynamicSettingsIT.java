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

    public void testAllIndexedSettingsCanBeUpdatedDynamically() {
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put("datafusion.indexed.batch_size", 16384)
                    .put("datafusion.indexed.parquet_pushdown_filters", true)
                    .put("datafusion.indexed.min_skip_run_default", 2048)
                    .put("datafusion.indexed.min_skip_run_selectivity_threshold", 0.5)
                    .put("datafusion.indexed.single_collector_strategy", "full_range")
                    .put("datafusion.indexed.tree_collector_strategy", "page_range_split")
                    .put("datafusion.indexed.max_collector_parallelism", 4)
                    .build()
            )
            .get();
        assertTrue(response.isAcknowledged());

        Settings transientSettings = response.getTransientSettings();
        assertEquals("16384", transientSettings.get("datafusion.indexed.batch_size"));
        assertEquals("true", transientSettings.get("datafusion.indexed.parquet_pushdown_filters"));
        assertEquals("2048", transientSettings.get("datafusion.indexed.min_skip_run_default"));
        assertEquals("0.5", transientSettings.get("datafusion.indexed.min_skip_run_selectivity_threshold"));
        assertEquals("full_range", transientSettings.get("datafusion.indexed.single_collector_strategy"));
        assertEquals("page_range_split", transientSettings.get("datafusion.indexed.tree_collector_strategy"));
        assertEquals("4", transientSettings.get("datafusion.indexed.max_collector_parallelism"));
    }

    public void testInvalidValuesAreRejected() {
        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.indexed.batch_size", 0).build())
                .get()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.indexed.min_skip_run_selectivity_threshold", 1.5).build())
                .get()
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("datafusion.indexed.single_collector_strategy", "bogus").build())
                .get()
        );
    }
}
