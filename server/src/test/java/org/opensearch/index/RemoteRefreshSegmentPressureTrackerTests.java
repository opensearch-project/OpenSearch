/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import static org.mockito.Mockito.mock;

public class RemoteRefreshSegmentPressureTrackerTests extends OpenSearchTestCase {

    private RemoteRefreshSegmentPressureSettings pressureSettings;

    private ClusterService clusterService;

    private ThreadPool threadPool;

    private ShardId shardId;

    private RemoteRefreshSegmentPressureTracker pressureTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("remote_refresh_segment_pressure_settings_test");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool
        );
        pressureSettings = new RemoteRefreshSegmentPressureSettings(
            clusterService,
            Settings.EMPTY,
            mock(RemoteRefreshSegmentPressureService.class)
        );
        shardId = new ShardId("index", "uuid", 0);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testGetShardId() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        assertEquals(shardId, pressureTracker.getShardId());
    }

    public void testUpdateLocalRefreshSeqNo() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        long refreshSeqNo = 2;
        pressureTracker.updateLocalRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getLocalRefreshSeqNo());
    }

    public void testUpdateRemoteRefreshSeqNo() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        long refreshSeqNo = 4;
        pressureTracker.updateRemoteRefreshSeqNo(refreshSeqNo);
        assertEquals(refreshSeqNo, pressureTracker.getRemoteRefreshSeqNo());
    }

    public void testUpdateLocalRefreshTimeMs() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        long refreshTimeMs = System.nanoTime() / 1_000_000L + randomIntBetween(10, 100);
        pressureTracker.updateLocalRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getLocalRefreshTimeMs());
    }

    public void testUpdateRemoteRefreshTimeMs() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        long refreshTimeMs = System.nanoTime() / 1_000_000 + randomIntBetween(10, 100);
        pressureTracker.updateRemoteRefreshTimeMs(refreshTimeMs);
        assertEquals(refreshTimeMs, pressureTracker.getRemoteRefreshTimeMs());
    }

    public void testComputeSeqNoLagOnUpdate() {
        pressureTracker = new RemoteRefreshSegmentPressureTracker(shardId, pressureSettings);
        int localRefreshSeqNo = randomIntBetween(50, 100);
        int remoteRefreshSeqNo = randomIntBetween(20, 50);
        pressureTracker.updateLocalRefreshSeqNo(localRefreshSeqNo);
        assertEquals(localRefreshSeqNo, pressureTracker.getSeqNoLag());
        pressureTracker.updateRemoteRefreshSeqNo(remoteRefreshSeqNo);
        assertEquals(localRefreshSeqNo - remoteRefreshSeqNo, pressureTracker.getSeqNoLag());
    }
}
