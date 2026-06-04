/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * This class can be utilised to track the time based expiration events.
 * Clients should be more cautious with the expiry time as this class is not completely thread safe. This is intentional
 * as nanoSecond granularity level error for `lastAccessTimeInNanos` is tolerable and can be ignored.
 * @opensearch.internal
 */
public class TimeBasedExpiryTracker implements BooleanSupplier {
    private final LongSupplier nanoTimeSupplier;
    private volatile long lastAccessTimeInNanos;
    private final long expiryTimeInNanos;
    private static final long ONE_SEC = 1000_000_000;

    public TimeBasedExpiryTracker(LongSupplier nanoTimeSupplier) {
        this(nanoTimeSupplier, ONE_SEC);
    }

    public TimeBasedExpiryTracker(LongSupplier nanoTimeSupplier, long expiryTimeInNanos) {
        this.nanoTimeSupplier = nanoTimeSupplier;
        this.lastAccessTimeInNanos = nanoTimeSupplier.getAsLong();
        this.expiryTimeInNanos = expiryTimeInNanos;
    }

    @Override
    public boolean getAsBoolean() {
        final long currentTime = nanoTimeSupplier.getAsLong();
        final boolean isExpired = (currentTime - lastAccessTimeInNanos) > expiryTimeInNanos;
        if (isExpired) {
            lastAccessTimeInNanos = currentTime;
        }
        return isExpired;
    }
}
