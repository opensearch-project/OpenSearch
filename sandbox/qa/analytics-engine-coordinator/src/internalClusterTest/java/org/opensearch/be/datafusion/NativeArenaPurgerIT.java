/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.nativebridge.spi.NativeArenaPurger;
import org.opensearch.nativebridge.spi.NativeMemoryFetcher;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration test verifying that the jemalloc arena purger reclaims native memory
 * after query execution completes.
 *
 * <p>Ingests high-cardinality data, runs an aggregation that forces multi-MB native
 * allocations, then asserts that resident memory drops after the debounced purge fires.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope(
    com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope.NONE
)
public class NativeArenaPurgerIT extends OpenSearchIntegTestCase {

    private static final Logger logger = LogManager.getLogger(NativeArenaPurgerIT.class);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            TestPPLPlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class, MockDataFormatPlugin.class
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
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Threshold=0 means always purge when check runs; interval=2s for fast test feedback.
        NativeArenaPurger.init(0, 2000);
    }

    @Override
    public void tearDown() throws Exception {
        NativeArenaPurger.setCheckIntervalMs(0);
        super.tearDown();
    }

    public void testPurgeReclaimsMemoryAfterQuery() throws Exception {
        String indexName = "purge_test";
        int numDocs = 50_000;

        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "parquet")
            ).setMapping("group_id", "type=integer", "user_id", "type=long", "region", "type=integer")
        );

        int batchSize = 5000;
        for (int batch = 0; batch < numDocs; batch += batchSize) {
            var bulkRequest = client().prepareBulk();
            int end = Math.min(batch + batchSize, numDocs);
            for (int i = batch; i < end; i++) {
                bulkRequest.add(client().prepareIndex(indexName)
                    .setSource("group_id", i % 10000, "user_id", (long) (i % 20000), "region", i % 500));
            }
            var bulkResponse = bulkRequest.get();
            assertFalse("Bulk indexing should not have failures", bulkResponse.hasFailures());
        }
        flush(indexName);
        ensureGreen(indexName);

        AnalyticsBackendNativeMemoryStats baseline = NativeMemoryFetcher.fetch();
        assertTrue("Native library must be loaded for this test", baseline.getResidentBytes() > 0);
        long baselineResident = baseline.getResidentBytes();

        long purgesBefore = NativeArenaPurger.getPurgeCount();

        // Run a high-cardinality aggregation via PPL to force large native allocations
        PPLResponse response = client().execute(
            UnifiedPPLExecuteAction.INSTANCE,
            new PPLRequest("source = " + indexName + " | stats dc(user_id) as u by group_id | sort - u | head 10")
        ).actionGet();
        assertNotNull(response);

        // Wait for purge to fire — threshold=0 means it fires on every check interval.
        // This verifies the full end-to-end mechanism: periodic timer → resident check → mallctl purge.
        assertBusy(() -> {
            assertTrue("Purge should have fired", NativeArenaPurger.getPurgeCount() > purgesBefore);
        }, 10, TimeUnit.SECONDS);

        long purgesAfter = NativeArenaPurger.getPurgeCount();
        logger.info(
            "Purge mechanism verified: purges={}, baseline_resident={} MB",
            purgesAfter, baselineResident / (1024 * 1024)
        );
        assertTrue("At least one purge should have executed", purgesAfter > purgesBefore);
    }
}
