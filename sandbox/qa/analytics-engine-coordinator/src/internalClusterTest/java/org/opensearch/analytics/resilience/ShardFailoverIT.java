/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.resilience;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

/**
 * Replica failover IT: when the primary's node goes down mid-query, the planner's
 * {@code ShardTargetResolver} hands {@code ShardFragmentStageExecution} a
 * {@code ShardExecutionTarget} carrying the full shard iterator. On dispatch failure the
 * stage's {@code retargetForRetry} advances to the next copy (replica) and the scheduler
 * retries without surfacing an error.
 *
 * <p>Composite-parquet supports replica recovery only with remote-store + segment
 * replication (the doc-replication / translog phase-2 path throws
 * {@code unsupported_operation_exception: updates/deletes not supported}). Hence this IT
 * extends {@link RemoteStoreBaseIntegTestCase} — segment-replication-via-remote-store is
 * the working configuration. Mirrors the topology used by
 * {@code DataFormatAwarePeerRecoveryIT}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ShardFailoverIT extends RemoteStoreBaseIntegTestCase {

    private static final int VALUE = 7;
    private static final int DOCS = 20;
    private static final TimeValue QUERY_TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final String INDEX = "failover_idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                MockCommitterEnginePlugin.class,
                MockTransportService.TestPlugin.class,
                TestPPLPlugin.class
            )
        ).collect(Collectors.toList());
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
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
     * Isolate the primary's node from the coordinator mid-flight; the scheduler must retarget
     * to the surviving replica copy. Asserts the PPL aggregate returns the full row count.
     *
     * <p>Uses {@link NetworkDisruption} rather than {@code stopRandomNode} because the
     * DataFusion native Tokio runtime is JVM-global across nodes in {@code InternalTestCluster}
     * — a real node stop tears down the runtime for the surviving node too (see
     * {@code CoordinatorTopologyTestBase} class-level comment). Disruption keeps both
     * processes alive; the coordinator just sees the primary's node as unreachable.
     */
    @AwaitsFix(
        bugUrl = "Flaky: the test's MockCommitterEnginePlugin (InMemoryCommitter) doesn't replicate the"
            + " parquet catalog to the remote store, so the failover replica serves 0 rows and the PPL"
            + " aggregate is null (NPE). Needs a replicating non-Lucene committer for this IT."
    )
    public void testQuerySucceedsAfterPrimaryNodeIsolated() throws Exception {
        // 1 cluster-manager (also handles REST requests) + 2 data nodes (primary + replica
        // land on different nodes). Disrupting between cluster-manager and one data node
        // leaves the other data node fully accessible.
        String clusterManager = internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);

        Settings indexSettings = Settings.builder()
            .put(remoteStoreIndexSettings(1, 1))  // 1 replica, 1 shard, remote-store-backed
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
        CreateIndexResponse cr = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("value", "type=integer")
            .get();
        assertTrue(cr.isAcknowledged());

        ClusterHealthResponse green = client().admin()
            .cluster()
            .prepareHealth(INDEX)
            .setWaitForGreenStatus()
            .setTimeout(TimeValue.timeValueSeconds(60))
            .get();
        if (green.isTimedOut() || "RED".equals(green.getStatus().name())) {
            IndexShardRoutingTable rt = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .routingTable()
                .index(INDEX)
                .shard(0);
            throw new AssertionError(
                "Index did not reach green within 60s. health.status="
                    + green.getStatus()
                    + " timed_out="
                    + green.isTimedOut()
                    + " primary="
                    + rt.primaryShard()
                    + " replicas="
                    + rt.replicaShards()
            );
        }

        for (int i = 0; i < DOCS; i++) {
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("value", VALUE).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();

        String primaryNodeName = nodeHostingPrimary(INDEX);
        // Isolate primary's node from the cluster-manager (which is acting as coordinator).
        // First dispatch to primary fails with NodeNotConnectedException → retargetForRetry
        // pulls the replica copy → second dispatch lands on the replica's node → succeeds.
        NetworkDisruption disruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(Collections.singleton(clusterManager), Collections.singleton(primaryNodeName)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(disruption);
        disruption.startDisrupting();

        try {
            long expected = (long) DOCS * VALUE;
            PPLResponse r = client().execute(
                UnifiedPPLExecuteAction.INSTANCE,
                new PPLRequest("source = " + INDEX + " | stats sum(value) as total")
            ).actionGet(QUERY_TIMEOUT);
            int idx = r.getColumns().indexOf("total");
            assertEquals("must return exactly one aggregate row", 1, r.getRows().size());
            long actual = ((Number) r.getRows().get(0)[idx]).longValue();
            assertThat("aggregate must equal docs * VALUE (full count via replica)", actual, equalTo(expected));
        } finally {
            disruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
        }
    }

    // Cancellation IT: end-to-end cancellation behavior is already covered by
    // SearchCancellationIT.testCancelShardFragmentTaskTerminatesQuery (cancel-mid-flight,
    // task cascades, native resources released). The retry-skip-on-stage-cancellation
    // contract specific to this PR is pinned deterministically by
    // QuerySchedulerTests.testCancelledStageSkipsRetryAndPropagatesOriginalCause —
    // an IT for the race "cancel arrives while retargetForRetry would advance to next
    // copy" was attempted via MockTransportService-blocked fragment handler + cancel of
    // the parent PPL action, but couldn't be made deterministic: child shard tasks
    // don't materialize until the handler unblocks, so the cancel either lands too
    // early (no tasks to cancel) or after release (query completes normally).

    // Iterator-exhaustion / chained-retry / vanished-node-on-retry contracts are pinned
    // deterministically in ShardFragmentStageExecutionTests (unit). An IT for the "all
    // copies unreachable" path was tried with NetworkDisruption but the PPL action
    // appears to route around the cluster-manager-vs-data-nodes partition (request lands
    // on a still-reachable data node), so we can't deterministically force the iterator
    // to exhaust at the integration layer from here. Unit coverage already pins the
    // contract — re-attempt the integration negative case once we have a way to route the
    // request through a partitioned coordinator without it forwarding to a data node.

    private String nodeHostingPrimary(String index) {
        IndexShardRoutingTable routingTable = client().admin()
            .cluster()
            .prepareState()
            .get()
            .getState()
            .routingTable()
            .index(index)
            .shard(0);
        ShardRouting primary = routingTable.primaryShard();
        assertNotNull("primary must be allocated", primary);
        return client().admin().cluster().prepareState().get().getState().nodes().get(primary.currentNodeId()).getName();
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
}
