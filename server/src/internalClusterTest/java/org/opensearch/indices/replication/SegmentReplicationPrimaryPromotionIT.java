/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.replication.TransportReplicationAction;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.NRTReplicationEngine;
import org.opensearch.indices.replication.checkpoint.PublishCheckpointAction;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.RemoteTransportException;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationPrimaryPromotionIT extends SegmentReplicationBaseIT {
    private static boolean lockEnable;
    private static CountDownLatch indexLuceneLatch;
    private static CountDownLatch flushLatch;
    private static CountDownLatch refreshLatch;

    @Before
    public void setup() {
        lockEnable = false;
        indexLuceneLatch = new CountDownLatch(1);
        flushLatch = new CountDownLatch(1);
        refreshLatch = new CountDownLatch(1);
        internalCluster().startClusterManagerOnlyNode();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        List<Class<? extends Plugin>> plugins = super.getMockPlugins().stream()
            .filter(plugin -> !plugin.getName().contains("MockEngineFactoryPlugin"))
            .collect(java.util.stream.Collectors.toList());
        plugins.add(MockEnginePlugin.class);
        return plugins;
    }

    public static class MockEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.of(new MockEngineFactory());
        }
    }

    public static class MockEngineFactory implements EngineFactory {
        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            return config.isReadOnlyReplica() ? new MockNRTReplicationEngine(config) : new MockInternalEngine(config);
        }
    }

    public static class MockInternalEngine extends InternalEngine {
        MockInternalEngine(EngineConfig config) throws EngineException {
            super(config);
        }

        @Override
        protected long generateSeqNoForOperationOnPrimary(final Operation operation) {
            long seqNo = super.generateSeqNoForOperationOnPrimary(operation);
            try {
                if (lockEnable) {
                    flushLatch.countDown();
                    assertTrue("indexLuceneLatch timed out", indexLuceneLatch.await(30, TimeUnit.SECONDS));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return seqNo;
        }
    }

    public static class MockNRTReplicationEngine extends NRTReplicationEngine {
        MockNRTReplicationEngine(EngineConfig config) throws EngineException {
            super(config);
        }

        @Override
        public IndexResult index(Index index) throws IOException {
            IndexResult indexResult = super.index(index);
            if (lockEnable) {
                refreshLatch.countDown();
            }
            return indexResult;
        }
    }

    // Used to test that primary promotion does not result in data loss.
    public void testPrimaryStopped_ReplicaPromoted_no_data_loss() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put("index.refresh_interval", -1).build());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        lockEnable = true;
        Thread writeThread = new Thread(() -> { client().prepareIndex(INDEX_NAME).setId("2").setSource("foo2", "bar2").get(); });
        writeThread.start();
        assertTrue("flushLatch timed out", flushLatch.await(30, TimeUnit.SECONDS));

        flush(INDEX_NAME);

        waitForSearchableDocs(1, replica);

        // The exception is thrown unconditionally to simulate persistent publish-checkpoint failures
        // until the primary is stopped and the replica is promoted.
        MockTransportService replicaTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            replica
        ));
        replicaTransportService.addRequestHandlingBehavior(
            PublishCheckpointAction.ACTION_NAME + TransportReplicationAction.REPLICA_ACTION_SUFFIX,
            (handler, request, channel, task) -> {
                throw new RemoteTransportException("mock remote transport exception", new OpenSearchRejectedExecutionException());
            }
        );

        refresh(INDEX_NAME);
        waitForSearchableDocs(1, primary);
        indexLuceneLatch.countDown();
        assertTrue("refreshLatch timed out", refreshLatch.await(30, TimeUnit.SECONDS));
        writeThread.join(TimeUnit.SECONDS.toMillis(30));
        assertFalse("writeThread did not complete in time", writeThread.isAlive());

        logger.info("refresh index");
        refresh(INDEX_NAME);
        flush(INDEX_NAME);
        waitForSearchableDocs(2, primary);
        waitForSearchableDocs(1, replica);

        // stop the primary node - we only have one shard on here.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());

        refresh(INDEX_NAME);
        SearchResponse response = client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get();
        assertEquals(2L, response.getHits().getTotalHits().value());
        replicaTransportService.clearAllRules();
    }
}
