/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.noop;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * No-op {@link Counter}
 * {@opensearch.internal}
 */
@InternalApi
public class NoopCounter implements Counter {

    /**
     * No-op Counter instance
     */
    public final static NoopCounter INSTANCE = new NoopCounter();

    private NoopCounter() {}

    @Override
    public void add(double value) {

    }

    @Override
    public void add(double value, Tags tags) {

    }
}
