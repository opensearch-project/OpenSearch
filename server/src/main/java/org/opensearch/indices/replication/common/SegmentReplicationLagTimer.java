/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Wrapper class for Replication Timer which also tracks time elapsed since the timer was created.
 * Currently, this is being used to calculate
 * 1. Replication Lag: Total time taken by replica to sync after primary refreshed.
 * 2. Replication event time: Total time taken by replica to sync after primary published the checkpoint
 *                     (excludes the time spent by primary for uploading the segments to remote store).
 *
 * @opensearch.internal
 */
public class SegmentReplicationLagTimer extends ReplicationTimer {
    private long creationTime;

    public SegmentReplicationLagTimer() {
        super();
        creationTime = System.nanoTime();
    }

    public SegmentReplicationLagTimer(StreamInput in) throws IOException {
        super(in);
        creationTime = in.readVLong();
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(creationTime);
    }

    public long totalElapsedTime() {
        return TimeValue.nsecToMSec(Math.max(System.nanoTime() - creationTime, 0));
    }
}
