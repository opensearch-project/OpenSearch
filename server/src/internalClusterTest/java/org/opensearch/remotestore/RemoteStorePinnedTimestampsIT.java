/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.node.remotestore.RemoteStorePinnedTimestampService;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.REMOTE_STORE;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.repositories.fs.ReloadableFsRepository.REPOSITORIES_SLOWDOWN_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStorePinnedTimestampsIT extends RemoteStoreBaseIntegTestCase {
    static final String INDEX_NAME = "remote-store-test-idx-1";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        String segmentRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            REPOSITORY_NAME
        );

        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(segmentRepoTypeAttributeKey, ReloadableFsRepository.TYPE)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .build();
    }

    ActionListener<Void> noOpActionListener = new ActionListener<>() {
        @Override
        public void onResponse(Void unused) {
            // do nothing
        }

        @Override
        public void onFailure(Exception e) {
            fail();
        }
    };

    public void testTimestampPinUnpin() throws Exception {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp = RemoteStorePinnedTimestampService.getPinnedTimestamps();
        assertEquals(Set.of(), pinnedTimestampWithFetchTimestamp.v2());

        long timestamp1 = System.currentTimeMillis() + 30000L;
        long timestamp2 = System.currentTimeMillis() + 60000L;
        long timestamp3 = System.currentTimeMillis() + 900000L;
        remoteStorePinnedTimestampService.pinTimestamp(timestamp1, "ss2", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp2, "ss3", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp3, "ss4", noOpActionListener);

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        assertBusy(() -> {
            Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp_2 = RemoteStorePinnedTimestampService.getPinnedTimestamps();
            long lastFetchTimestamp_2 = pinnedTimestampWithFetchTimestamp_2.v1();
            assertTrue(lastFetchTimestamp_2 != -1);
            Map<String, List<Long>> pinnedEntities = RemoteStorePinnedTimestampService.getPinnedEntities();
            assertEquals(3, pinnedEntities.size());
            assertEquals(Set.of("ss2", "ss3", "ss4"), pinnedEntities.keySet());
            assertEquals(pinnedEntities.get("ss2").size(), 1);
            assertEquals(Optional.of(timestamp1).get(), pinnedEntities.get("ss2").get(0));
            assertEquals(Set.of(timestamp1, timestamp2, timestamp3), pinnedTimestampWithFetchTimestamp_2.v2());
        });

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));

        // Unpinning already pinned entity
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp2, "ss3", noOpActionListener);

        // This should fail as timestamp is not pinned by pinning entity
        CountDownLatch latch = new CountDownLatch(1);
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp1, "no-snapshot", new LatchedActionListener<>(new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                // onResponse should not get called.
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalArgumentException);
            }
        }, latch));
        latch.await();

        // Adding different entity to already pinned timestamp
        remoteStorePinnedTimestampService.pinTimestamp(timestamp3, "ss5", noOpActionListener);

        remoteStorePinnedTimestampService.forceSyncPinnedTimestamps();

        assertBusy(() -> {
            Tuple<Long, Set<Long>> pinnedTimestampWithFetchTimestamp_3 = RemoteStorePinnedTimestampService.getPinnedTimestamps();
            Map<String, List<Long>> pinnedEntities = RemoteStorePinnedTimestampService.getPinnedEntities();
            assertEquals(3, pinnedEntities.size());
            assertEquals(pinnedEntities.get("ss5").size(), 1);
            assertEquals(Optional.of(timestamp3).get(), pinnedEntities.get("ss5").get(0));
            long lastFetchTimestamp_3 = pinnedTimestampWithFetchTimestamp_3.v1();
            assertTrue(lastFetchTimestamp_3 != -1);
            assertEquals(Set.of(timestamp1, timestamp3), pinnedTimestampWithFetchTimestamp_3.v2());
        });

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));
    }

    public void testPinnedTimestampClone() throws Exception {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        long timestamp1 = System.currentTimeMillis() + 30000L;
        long timestamp2 = System.currentTimeMillis() + 60000L;
        long timestamp3 = System.currentTimeMillis() + 900000L;
        remoteStorePinnedTimestampService.pinTimestamp(timestamp1, "ss2", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp2, "ss3", noOpActionListener);
        remoteStorePinnedTimestampService.pinTimestamp(timestamp3, "ss4", noOpActionListener);

        // Clone timestamp1
        remoteStorePinnedTimestampService.cloneTimestamp(timestamp1, "ss2", "ss2-2", noOpActionListener);

        // With clone, set of pinned timestamp will not change
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));
        assertBusy(
            () -> assertEquals(Set.of(timestamp1, timestamp2, timestamp3), RemoteStorePinnedTimestampService.getPinnedTimestamps().v2())
        );
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));

        // Clone timestamp1 but provide invalid existing entity
        CountDownLatch latch = new CountDownLatch(1);
        remoteStorePinnedTimestampService.cloneTimestamp(
            timestamp1,
            "ss3",
            "ss2-3",
            new LatchedActionListener<>(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    // onResponse should not get called.
                    fail();
                }

                @Override
                public void onFailure(Exception e) {
                    assertTrue(e instanceof IllegalArgumentException);
                }
            }, latch)
        );
        latch.await();

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));
        assertBusy(
            () -> assertEquals(Set.of(timestamp1, timestamp2, timestamp3), RemoteStorePinnedTimestampService.getPinnedTimestamps().v2())
        );
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));

        // Now we have timestamp1 pinned by 2 entities, unpin 1, this should not change set of pinned timestamps
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp1, "ss2", noOpActionListener);

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));
        assertBusy(
            () -> assertEquals(Set.of(timestamp1, timestamp2, timestamp3), RemoteStorePinnedTimestampService.getPinnedTimestamps().v2())
        );
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));

        // Now unpin second entity as well, set of pinned timestamp should be reduced by 1
        remoteStorePinnedTimestampService.unpinTimestamp(timestamp1, "ss2-2", noOpActionListener);

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));
        assertBusy(() -> assertEquals(Set.of(timestamp2, timestamp3), RemoteStorePinnedTimestampService.getPinnedTimestamps().v2()));
        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));
    }

    public void testPinExceptionsOlderTimestamp() throws InterruptedException {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        CountDownLatch latch = new CountDownLatch(1);
        remoteStorePinnedTimestampService.pinTimestamp(1234L, "ss1", new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // We expect onFailure to be called
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalArgumentException);
            }
        }, latch));

        latch.await();
    }

    public void testPinExceptionsRemoteStoreCallTakeTime() throws InterruptedException, ExecutionException {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        CountDownLatch latch = new CountDownLatch(1);
        slowDownRepo(REPOSITORY_NAME, 10);
        RemoteStoreSettings.setPinnedTimestampsLookbackInterval(TimeValue.timeValueSeconds(1));
        long timestampToBePinned = System.currentTimeMillis() + 600000;
        remoteStorePinnedTimestampService.pinTimestamp(timestampToBePinned, "ss1", new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // We expect onFailure to be called
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e.getMessage());
                assertTrue(e instanceof RuntimeException);
                assertTrue(e.getMessage().contains("Timestamp pinning took"));

                // Check if the timestamp was unpinned
                remoteStorePinnedTimestampService.forceSyncPinnedTimestamps();
                assertFalse(RemoteStorePinnedTimestampService.getPinnedTimestamps().v2().contains(timestampToBePinned));
            }
        }, latch));

        latch.await();
    }

    protected void slowDownRepo(String repoName, int value) throws ExecutionException, InterruptedException {
        GetRepositoriesRequest gr = new GetRepositoriesRequest(new String[] { repoName });
        GetRepositoriesResponse res = client().admin().cluster().getRepositories(gr).get();
        RepositoryMetadata rmd = res.repositories().get(0);
        Settings.Builder settings = Settings.builder()
            .put("location", rmd.settings().get("location"))
            .put(REPOSITORIES_SLOWDOWN_SETTING.getKey(), value);
        createRepository(repoName, rmd.type(), settings);
    }

    public void testUnpinException() throws InterruptedException {
        prepareCluster(1, 1, INDEX_NAME, 0, 2);
        ensureGreen(INDEX_NAME);

        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            primaryNodeName(INDEX_NAME)
        );

        CountDownLatch latch = new CountDownLatch(1);
        remoteStorePinnedTimestampService.unpinTimestamp(1234L, "dummy-entity", new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                // We expect onFailure to be called
                fail();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e.getMessage());
                assertTrue(e instanceof IllegalArgumentException);
            }
        }, latch));

        latch.await();
    }

    public void testLastSuccessfulFetchOfPinnedTimestampsPresentInNodeStats() throws Exception {
        logger.info("Starting up cluster manager");
        logger.info("cluster.remote_store.pinned_timestamps.enabled set to true");
        logger.info("cluster.remote_store.pinned_timestamps.scheduler_interval set to minimum value of 1minute");
        Settings pinnedTimestampEnabledSettings = Settings.builder()
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), true)
            .put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_SCHEDULER_INTERVAL.getKey(), "1m")
            .build();
        internalCluster().startClusterManagerOnlyNode(pinnedTimestampEnabledSettings);
        String remoteNodeName = internalCluster().startDataOnlyNodes(1, pinnedTimestampEnabledSettings).get(0);
        ensureStableCluster(2);
        RemoteStorePinnedTimestampService remoteStorePinnedTimestampService = internalCluster().getInstance(
            RemoteStorePinnedTimestampService.class,
            remoteNodeName
        );

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueSeconds(1));

        assertBusy(() -> {
            long lastSuccessfulFetchOfPinnedTimestamps = RemoteStorePinnedTimestampService.getPinnedTimestamps().v1();
            assertTrue(lastSuccessfulFetchOfPinnedTimestamps > 0L);
            NodesStatsResponse nodesStatsResponse = internalCluster().client()
                .admin()
                .cluster()
                .prepareNodesStats()
                .addMetric(REMOTE_STORE.metricName())
                .execute()
                .actionGet();
            for (NodeStats nodeStats : nodesStatsResponse.getNodes()) {
                long lastRecordedFetch = nodeStats.getRemoteStoreNodeStats().getLastSuccessfulFetchOfPinnedTimestamps();
                assertTrue(lastRecordedFetch >= lastSuccessfulFetchOfPinnedTimestamps);
            }
        });

        remoteStorePinnedTimestampService.rescheduleAsyncUpdatePinnedTimestampTask(TimeValue.timeValueMinutes(3));
    }
}
