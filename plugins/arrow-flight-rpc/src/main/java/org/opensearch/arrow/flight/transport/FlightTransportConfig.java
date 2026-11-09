/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared configuration for Flight transport components.
 */
class FlightTransportConfig {
    private final AtomicReference<TimeValue> slowLogThreshold = new AtomicReference<>(TimeValue.timeValueMillis(5000));
    private final AtomicReference<TimeValue> streamCloseTimeout = new AtomicReference<>(TimeValue.timeValueSeconds(5));

    public TimeValue getSlowLogThreshold() {
        return slowLogThreshold.get();
    }

    public void setSlowLogThreshold(TimeValue threshold) {
        slowLogThreshold.set(threshold);
    }

    /**
     * How long a client channel close waits for active stream consumers to release their streams'
     * buffers before closing the underlying flight client.
     */
    public TimeValue getStreamCloseTimeout() {
        return streamCloseTimeout.get();
    }

    public void setStreamCloseTimeout(TimeValue timeout) {
        streamCloseTimeout.set(timeout);
    }
}
