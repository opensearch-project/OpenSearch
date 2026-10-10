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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scratch probe — prints actual rows and dispatch counts for the four can-match query shapes
 * (filter only / sort+head / filter+sort+head / head only) so the behaviour can be eyeballed.
 * Correctness lives in {@link SortEarlyTerminationIT}; this exists to show output.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(
    scope = OpenSearchIntegTestCase.Scope.SUITE,
    numDataNodes = 1,
    numClientNodes = 0,
    supportsDedicatedMasters = false
)
public class QueryShapeProbeIT extends OpenSearchIntegTestCase {

    private static final int DAYS = 5;      // 2026-07-10 .. 2026-07-14
    private static final int FIRST_DAY = 10;
    private static final int DOCS_PER_DAY = 4;

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
            .put(AnalyticsQuerySettings.MAX_CONCURRENT_SHARD_REQUESTS_PER_NODE.getKey(), 1)
            .build();
    }

    public void testPrintQueryShapes() {
        String idx = createDailyIndices();
        AtomicInteger dispatches = countFragmentDispatches();
        try {
            probe("1. filter only (pruning, no gate)", "source = " + idx + " | where `ts` >= '2026-07-13 00:00:00' | fields ts, host", dispatches);

            probe("2. sort + head (gate, no pruning)", "source = " + idx + " | sort - ts | head 5 | fields ts", dispatches);

            probe(
                "3. filter + sort + head (both)",
                "source = " + idx + " | where `ts` >= '2026-07-12 00:00:00' | sort - ts | head 5 | fields ts",
                dispatches
            );

            probe("4. head only (no sort, nothing gateable)", "source = " + idx + " | head 5 | fields ts", dispatches);

            probe("5. sort + head ascending", "source = " + idx + " | sort + ts | head 5 | fields ts", dispatches);

            probe(
                "6. filter + sort + head, fetch-only column (QTF/LM path)",
                "source = " + idx + " | where `ts` >= '2026-07-12 00:00:00' | sort - ts | head 5 | fields ts, host",
                dispatches
            );
        } finally {
            clearTransportRules();
        }
    }

    /** Runs one PPL and logs the rows it returned plus how many shards it opened. */
    private void probe(String label, String ppl, AtomicInteger dispatches) {
        logger.info("PROBE ================================================================");
        logger.info("PROBE {}", label);
        logger.info("PROBE   {}", ppl);

        dispatches.set(0);
        List<String> rows = renderRows(executePPL(ppl));
        int sent = dispatches.getAndSet(0);

        logger.info("PROBE   dispatches: {} (of {} shards)", sent, DAYS);
        logger.info("PROBE   rows ({}):", rows.size());
        for (String row : rows) {
            logger.info("PROBE     {}", row);
        }
    }

    private static List<String> renderRows(PPLResponse response) {
        List<String> rendered = new ArrayList<>(response.getRows().size());
        for (Object[] row : response.getRows()) {
            rendered.add(Arrays.deepToString(row));
        }
        return rendered;
    }

    private String createDailyIndices() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        StringBuilder commaList = new StringBuilder();
        for (int d = FIRST_DAY; d < FIRST_DAY + DAYS; d++) {
            String index = String.format(Locale.ROOT, "probe-2026-07-%02d", d);
            CreateIndexResponse response = client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(settings)
                .setMapping("ts", "type=date", "host", "type=keyword,index=false")
                .get();
            assertTrue(response.isAcknowledged());

            for (int i = 0; i < DOCS_PER_DAY; i++) {
                client().prepareIndex(index)
                    .setSource(
                        "ts",
                        String.format(Locale.ROOT, "2026-07-%02dT%02d:00:00Z", d, i * 6),
                        "host",
                        String.format(Locale.ROOT, "host-%02d-%d", d, i)
                    )
                    .get();
            }
            client().admin().indices().prepareRefresh(index).get();
            client().admin().indices().prepareFlush(index).get();

            if (d > FIRST_DAY) {
                commaList.append(',');
            }
            commaList.append(index);
        }
        ensureGreen();
        return commaList.toString();
    }

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

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }
}
