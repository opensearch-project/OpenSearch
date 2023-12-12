/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class RemoteTranslogStatsTests extends OpenSearchTestCase {
    RemoteTranslogTransferTracker.Stats transferTrackerStats;
    RemoteTranslogStats remoteTranslogStats;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transferTrackerStats = getRandomTransferTrackerStats();
        remoteTranslogStats = new RemoteTranslogStats(transferTrackerStats);
    }

    public void testRemoteTranslogStatsCreationFromTransferTrackerStats() {
        assertEquals(transferTrackerStats.totalUploadsStarted, remoteTranslogStats.getTotalUploadsStarted());
        assertEquals(transferTrackerStats.totalUploadsSucceeded, remoteTranslogStats.getTotalUploadsSucceeded());
        assertEquals(transferTrackerStats.totalUploadsFailed, remoteTranslogStats.getTotalUploadsFailed());
        assertEquals(transferTrackerStats.uploadBytesStarted, remoteTranslogStats.getUploadBytesStarted());
        assertEquals(transferTrackerStats.uploadBytesSucceeded, remoteTranslogStats.getUploadBytesSucceeded());
        assertEquals(transferTrackerStats.uploadBytesFailed, remoteTranslogStats.getUploadBytesFailed());
    }

    public void testRemoteTranslogStatsSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            remoteTranslogStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                RemoteTranslogStats remoteTranslogStatsFromStream = new RemoteTranslogStats(in);
                assertEquals(remoteTranslogStats, remoteTranslogStatsFromStream);
            }
        }
    }

    public void testAdd() {
        RemoteTranslogTransferTracker.Stats otherTransferTrackerStats = getRandomTransferTrackerStats();
        RemoteTranslogStats otherRemoteTranslogStats = new RemoteTranslogStats(otherTransferTrackerStats);

        otherRemoteTranslogStats.add(remoteTranslogStats);

        assertEquals(
            otherRemoteTranslogStats.getTotalUploadsStarted(),
            otherTransferTrackerStats.totalUploadsStarted + remoteTranslogStats.getTotalUploadsStarted()
        );
        assertEquals(
            otherRemoteTranslogStats.getTotalUploadsSucceeded(),
            otherTransferTrackerStats.totalUploadsSucceeded + remoteTranslogStats.getTotalUploadsSucceeded()
        );
        assertEquals(
            otherRemoteTranslogStats.getTotalUploadsFailed(),
            otherTransferTrackerStats.totalUploadsFailed + remoteTranslogStats.getTotalUploadsFailed()
        );
        assertEquals(
            otherRemoteTranslogStats.getUploadBytesStarted(),
            otherTransferTrackerStats.uploadBytesStarted + remoteTranslogStats.getUploadBytesStarted()
        );
        assertEquals(
            otherRemoteTranslogStats.getUploadBytesSucceeded(),
            otherTransferTrackerStats.uploadBytesSucceeded + remoteTranslogStats.getUploadBytesSucceeded()
        );
        assertEquals(
            otherRemoteTranslogStats.getUploadBytesFailed(),
            otherTransferTrackerStats.uploadBytesFailed + remoteTranslogStats.getUploadBytesFailed()
        );
    }

    private static RemoteTranslogTransferTracker.Stats getRandomTransferTrackerStats() {
        return new RemoteTranslogTransferTracker.Stats(
            new ShardId("test-idx", "test-idx", randomIntBetween(1, 10)),
            0L,
            randomLongBetween(100, 500),
            randomLongBetween(50, 100),
            randomLongBetween(100, 200),
            randomLongBetween(10000, 50000),
            randomLongBetween(5000, 10000),
            randomLongBetween(10000, 20000),
            0L,
            0D,
            0D,
            0D,
            0L,
            0L,
            0L,
            0L,
            0D,
            0D,
            0D
        );
    }
}
