/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.plugin.ArrowBasePlugin;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;

/**
 * Shared plugin / settings boilerplate for topology-only coordinator ITs. Each subclass
 * lives in its own file because {@code @ClusterScope(numDataNodes = N)} is class-level
 * and the Tokio runtime manager is JVM-global — mid-test {@code stopRandomNode}
 * would shut it down for the whole JVM, so topology variants cannot share a class
 * with {@code CoordinatorResilienceIT}.
 */
public abstract class CoordinatorTopologyTestBase extends OpenSearchIntegTestCase {

    protected static final int VALUE = 7;
    protected static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

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
     * Creates a composite-parquet index with {@code numShards} primaries, seeds {@code docs}
     * rows of {@code value=VALUE}, then runs {@code stats sum(value)} via PPL and asserts
     * the SUM equals {@code docs * VALUE}.
     */
    protected void runSumOverSeededIndex(String index, int numShards, int docs) throws Exception {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
        CreateIndexResponse cr = client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue(cr.isAcknowledged());
        ensureGreen(index);

        for (int i = 0; i < docs; i++) {
            client().prepareIndex(index).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(index).get();
        client().admin().indices().prepareFlush(index).get();

        long expected = (long) docs * VALUE;
        try {
            assertBusy(() -> {
                PPLResponse r = client().execute(
                    UnifiedPPLExecuteAction.INSTANCE,
                    new PPLRequest("source = " + index + " | stats sum(value) as total")
                ).actionGet(QUERY_TIMEOUT);
                int idx = r.getColumns().indexOf("total");
                assertEquals(1, r.getRows().size());
                long actual = ((Number) r.getRows().get(0)[idx]).longValue();
                assertThat(actual, equalTo(expected));
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("topology SUM query failed for index=" + index + " shards=" + numShards + " docs=" + docs, e);
        }
    }
}
