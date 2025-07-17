/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Streak is a data structure that keeps track of the number of successive successful events.
 *
 * @opensearch.internal
 */
public class Streak {
    private final AtomicInteger successiveSuccessfulEvents = new AtomicInteger();

    public int record(boolean isSuccessful) {
        if (isSuccessful) {
            return successiveSuccessfulEvents.incrementAndGet();
        } else {
            successiveSuccessfulEvents.set(0);
            return 0;
        }
    }

    public int length() {
        return successiveSuccessfulEvents.get();
    }
}
