/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.diagnostics;

import org.opensearch.telemetry.metrics.MetricPoint;

/**
 * Reports observed {@link MetricPoint} of a resource. It is supposed to be a singleton class
 */
public interface ThreadResourceObserver {
    /**
     * Observed gauge associated with a resource
     * Multiple threads can call observe at same time, ensure the thread safety among shared state if any
     * @param t thread to be observed
     * @return observed usage
     */
    MetricPoint observe(Thread t);
}
