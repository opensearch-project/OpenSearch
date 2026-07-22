package org.opensearch.cluster.routing;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryReplicaResyncTimeoutIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    public void testPrimaryReplicaResyncTimeoutClearsFlag() throws Exception {
        logger.info("--> Starting 1 cluster manager and 3 data nodes");
        String clusterManagerName = internalCluster().startClusterManagerOnlyNode(Settings.EMPTY);
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        final String nodeC = internalCluster().startDataOnlyNode();

        // Use a short resync timeout for the test
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("indices.replication.retry_timeout", "2s"))
                .get()
        );

        logger.info("--> Creating index pinned to nodeA and nodeB");
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, 1)
                .put(SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.routing.allocation.include._name", nodeA + "," + nodeB)
                .put("index.routing.allocation.exclude._name", nodeC)
        ).get();

        ensureGreen("test");

        logger.info("--> Indexing some documents and flushing");
        for (int i = 0; i < 100; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        flush("test");

        logger.info("--> Discovering nodes");
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        String primaryNodeId = state.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        String primaryNodeName = state.nodes().get(primaryNodeId).getName();
        String replicaNodeName = primaryNodeName.equals(nodeA) ? nodeB : nodeA;

        logger.info("--> Mock request-handling drop on the future primary");
        MockTransportService futurePrimaryTransport = (MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replicaNodeName
        );
        futurePrimaryTransport.addRequestHandlingBehavior("internal:index/seq_no/resync[p]", (handler, request, channel, task) -> {
            // Silently consume request to simulate lost/hung transport response
            logger.info("--> Intercepted and silently dropped internal:index/seq_no/resync[p]");
        });

        logger.info("--> Stopping the current primary to trigger promotion of the replica");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNodeName));

        assertBusy(() -> {
            ClusterState s = client().admin().cluster().prepareState().get().getState();
            String pNode = s.routingTable().index("test").shard(0).primaryShard().currentNodeId();
            assertEquals(replicaNodeName, s.nodes().get(pNode).getName());
        });

        logger.info("--> Verifying the resync flag on the promoted primary is set");
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, replicaNodeName);
        IndexShard primaryShard = indicesService.getShardOrNull(
            new org.opensearch.core.index.shard.ShardId(state.metadata().index("test").getIndex(), 0)
        );
        java.lang.reflect.Field field = IndexShard.class.getDeclaredField("primaryReplicaResyncInProgress");
        field.setAccessible(true);
        AtomicBoolean flag = (AtomicBoolean) field.get(primaryShard);
        assertTrue(flag.get());

        logger.info("--> Waiting for timeout to fire (configured at 2 seconds)");
        assertBusy(() -> { assertFalse("Resync flag should be cleared after timeout", flag.get()); });

        logger.info("--> Wait for primary shard to restart/recover after failure");
        ensureYellow("test");

        logger.info("--> Opening allocation to nodeC");
        client().admin()
            .indices()
            .prepareUpdateSettings("test")
            .setSettings(
                Settings.builder()
                    .put("index.routing.allocation.include._name", replicaNodeName + "," + nodeC)
                    .put("index.routing.allocation.exclude._name", "")
            )
            .get();

        logger.info("--> Issuing reroute/move command");
        client().admin().cluster().prepareReroute().add(new MoveAllocationCommand("test", 0, replicaNodeName, nodeC)).get();

        // The relocation should succeed/start now that the flag is false
        assertBusy(() -> {
            ClusterState s = client().admin().cluster().prepareState().get().getState();
            assertTrue(s.routingTable().index("test").shard(0).primaryShard().relocating());
        });
    }
}
