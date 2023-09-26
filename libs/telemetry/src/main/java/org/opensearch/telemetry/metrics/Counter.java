/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.telemetry.tracing.attributes.Attributes;

/**
 * Counter adds the value to the existing metric.
 */
public interface Counter {

    /**
     * add value.
     * @param value value to be added.
     */
    void add(double value);

    /**
     * add value along with the attributes.
     * @param value value to be added.
     * @param attributes attributes/dimensions of the metric.
     */
    void add(double value, Attributes attributes);

}
