/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.telemetry.OTelAttributesConverter;
import org.opensearch.telemetry.metrics.tags.Tags;

import io.opentelemetry.api.metrics.DoubleCounter;

/**
 * OTel Counter
 */
public class OTelCounter implements Counter {

    private final DoubleCounter otelDoubleCounter;

    /**
     * Constructor
     * @param otelDoubleCounter delegate counter.
     */
    public OTelCounter(DoubleCounter otelDoubleCounter) {
        this.otelDoubleCounter = otelDoubleCounter;
    }

    @Override
    public void add(double value) {
        otelDoubleCounter.add(value);
    }

    @Override
    public void add(double value, Tags tags) {
        otelDoubleCounter.add(value, OTelAttributesConverter.convert(tags));
    }
}
