/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

public class Timer implements Writeable {
    public long startTime = 0;
    public long startNanoTime = 0;
    public long time = -1;
    public long stopTime = 0;

    public Timer() {}

    public Timer(StreamInput in) throws IOException {
        startTime = in.readVLong();
        startNanoTime = in.readVLong();
        stopTime = in.readVLong();
        time = in.readVLong();
    }

    @Override
    public synchronized void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startTime);
        out.writeVLong(startNanoTime);
        out.writeVLong(stopTime);
        // write a snapshot of current time, which is not per se the time field
        out.writeVLong(time());
    }

    public synchronized void start() {
        assert startTime == 0 : "already started";
        startTime = System.currentTimeMillis();
        startNanoTime = System.nanoTime();
    }

    /**
     * Returns start time in millis
     */
    public synchronized long startTime() {
        return startTime;
    }

    /**
     * Returns elapsed time in millis, or 0 if timer was not started
     */
    public synchronized long time() {
        if (startNanoTime == 0) {
            return 0;
        }
        if (time >= 0) {
            return time;
        }
        return Math.max(0, TimeValue.nsecToMSec(System.nanoTime() - startNanoTime));
    }

    /**
     * Returns stop time in millis
     */
    public synchronized long stopTime() {
        return stopTime;
    }

    public synchronized void stop() {
        assert stopTime == 0 : "already stopped";
        stopTime = Math.max(System.currentTimeMillis(), startTime);
        time = TimeValue.nsecToMSec(System.nanoTime() - startNanoTime);
        assert time >= 0;
    }

    public synchronized void reset() {
        startTime = 0;
        startNanoTime = 0;
        time = -1;
        stopTime = 0;
    }

    // for tests
    public long getStartNanoTime() {
        return startNanoTime;
    }
}
