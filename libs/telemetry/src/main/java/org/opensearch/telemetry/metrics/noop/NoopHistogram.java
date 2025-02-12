/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.noop;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.tags.Tags;

/**
 * No-op {@link Histogram}
 * {@opensearch.internal}
 */
@InternalApi
public class NoopHistogram implements Histogram {

    /**
     * No-op Histogram instance
     */
    public final static NoopHistogram INSTANCE = new NoopHistogram();

    private NoopHistogram() {}

    @Override
    public void record(double value) {

    }

    @Override
    public void record(double value, Tags tags) {

    }
}
