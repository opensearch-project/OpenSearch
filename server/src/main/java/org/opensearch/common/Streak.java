/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streak is a data structure that keeps track of the number of consecutive successful events.
 *
 * @opensearch.internal
 */
public class Streak {
    private final AtomicInteger consecutiveSuccessfulEvents = new AtomicInteger();

    public int record(boolean isSuccessful) {
        if (isSuccessful) {
            return consecutiveSuccessfulEvents.incrementAndGet();
        } else {
            consecutiveSuccessfulEvents.set(0);
            return 0;
        }
    }

    public int length() {
        return consecutiveSuccessfulEvents.get();
    }
}
