/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.BackpressureStrategy;

/**
 * Base class for backpressure strategy.
 */
public class BaseBackpressureStrategy extends BackpressureStrategy.CallbackBackpressureStrategy {
    private final Runnable readyCallback;
    private final Runnable cancelCallback;

    /**
     * Constructor for BaseBackpressureStrategy.
     *
     * @param readyCallback Callback to execute when the listener is ready.
     * @param cancelCallback Callback to execute when the listener is cancelled.
     */
    BaseBackpressureStrategy(Runnable readyCallback, Runnable cancelCallback) {
        this.readyCallback = readyCallback;
        this.cancelCallback = cancelCallback;
    }

    /** Callback to execute when the listener is ready. */
    protected void readyCallback() {
        if (readyCallback != null) {
            readyCallback.run();
        }
    }

    /** Callback to execute when the listener is cancelled. */
    protected void cancelCallback() {
        if (cancelCallback != null) {
            cancelCallback.run();
        }
    }
}
