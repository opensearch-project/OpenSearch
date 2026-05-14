/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.action.FragmentExecutionAction;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
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
import org.opensearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Demonstrates {@link MockTransportService#addRequestHandlingBehavior} routing
 * to handlers registered on the streaming transport. The analytics-engine
 * registers {@link FragmentExecutionAction#NAME} on
 * {@link org.opensearch.transport.StreamTransportService} when streaming is
 * enabled (its handler runs on the streaming side, not the regular transport),
 * so test-only request stubbing previously had no way to intercept it.
 *
 * <p>The change in this PR makes {@code addRequestHandlingBehavior} fall back
 * to the streaming-transport's stub registry when the action is not found in
 * the regular transport. This IT exercises that path end-to-end.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3, numClientNodes = 0)
public class CoordinatorResilienceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "resilience_idx";
    private static final int NUM_SHARDS = 3;
    private static final int DOCS_PER_SHARD_TARGET = 10;
    private static final int VALUE = 7;
    private static final int TOTAL_DOCS = NUM_SHARDS * DOCS_PER_SHARD_TARGET;
    private static final long EXPECTED_SUM = (long) TOTAL_DOCS * VALUE;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);

    private BufferAllocator stubAllocator;

    @After
    public void closeStubAllocator() {
        if (stubAllocator != null) {
            stubAllocator.close();
            stubAllocator = null;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            TestPPLPlugin.class,
            FlightStreamPlugin.class,
            CompositeDataFormatPlugin.class,
            MockTransportService.TestPlugin.class,
            // Stub committer factory satisfies the EngineConfigFactory boot-time
            // check (`committerFactories.isEmpty() && isPluggableDataFormatEnabled`)
            // without pulling the Lucene backend onto the IT classpath.
            MockCommitterEnginePlugin.class
        );
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
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /**
     * Stubs one shard's {@link FragmentExecutionAction} entirely — no real
     * handler runs, the data node never produces real data. Instead the stub
     * returns a single zero-row Arrow batch carrying a minimal schema, then
     * completes the stream. Coordinator must still produce a valid (smaller)
     * result from the other two shards.
     *
     * <p>Exercises the streaming-fallback path in {@link MockTransportService}:
     * {@link FragmentExecutionAction#NAME} is registered only on the streaming
     * transport, so without the fallback the stub would never bind.
     */
    public void testStubReplacesStreamingShardResponseWithEmptyBatch() throws Exception {
        createAndSeedIndex();
        stubAllocator = new RootAllocator();
        // Schema width must match the coordinator's declared input-partition schema — that's
        // the *aggregate* output type (SUM(int) → Int64/BIGINT), not the base column type.
        Schema schema = new Schema(List.of(new Field("total", FieldType.nullable(new ArrowType.Int(64, true)), null)));

        AtomicInteger stubCalls = new AtomicInteger();
        String victim = pickShardHostingNode();
        MockTransportService mts = (MockTransportService) internalCluster().getInstance(TransportService.class, victim);
        mts.addRequestHandlingBehavior(FragmentExecutionAction.NAME, (handler, request, channel, task) -> {
            stubCalls.incrementAndGet();
            VectorSchemaRoot vsr = VectorSchemaRoot.create(schema, stubAllocator);
            vsr.allocateNew();
            vsr.setRowCount(0);
            // sendResponseBatch transfers buffer ownership to the wire. Honors the Flight protocol
            // invariant that ≥1 schema-bearing frame must precede completeStream.
            channel.sendResponseBatch(new FragmentExecutionArrowResponse(vsr));
            channel.completeStream();
        });
        try {
            PPLResponse response = executePPL("source = " + INDEX + " | stats sum(value) as total", QUERY_TIMEOUT);
            assertThat("stub must fire on the streaming-only fragment action", stubCalls.get(), greaterThan(0));
            assertNotNull("coordinator must produce a response when one shard contributes nothing", response);
            long actual = ((Number) response.getRows().get(0)[response.getColumns().indexOf("total")]).longValue();
            assertThat("Partial sum must be < full when a shard contributes nothing; got " + actual, actual, lessThan(EXPECTED_SUM));
            assertThat("Partial sum must be ≥ 0 given the other two shards' contribution", actual, greaterThan(-1L));
        } finally {
            mts.clearAllRules();
        }
    }

    /**
     * Creates + seeds the test index. Composite-parquet flush-durability is
     * not synchronous with prepareFlush().get(), so we assertBusy on the
     * analytics-path sum until the seed is visible.
     */
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
            .setMapping("value", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        for (int i = 0; i < TOTAL_DOCS; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
        try {
            assertBusy(() -> {
                PPLResponse r = executePPL("source = " + INDEX + " | stats sum(value) as total");
                long actual = ((Number) r.getRows().get(0)[r.getColumns().indexOf("total")]).longValue();
                assertEquals("seed not yet visible to analytics path", EXPECTED_SUM, actual);
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("createAndSeedIndex: timed out waiting for seed durability", e);
        }
    }

    private PPLResponse executePPL(String ppl) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet();
    }

    private PPLResponse executePPL(String ppl, TimeValue timeout) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(ppl)).actionGet(timeout);
    }

    /** Return one node name that currently hosts a primary of {@link #INDEX}. */
    private String pickShardHostingNode() {
        Map<Integer, String> out = new HashMap<>();
        for (ShardRouting sr : clusterService().state()
            .routingTable()
            .index(INDEX)
            .shardsWithState(org.opensearch.cluster.routing.ShardRoutingState.STARTED)) {
            if (sr.primary()) {
                out.put(sr.id(), clusterService().state().nodes().get(sr.currentNodeId()).getName());
            }
        }
        return out.values().iterator().next();
    }
}
