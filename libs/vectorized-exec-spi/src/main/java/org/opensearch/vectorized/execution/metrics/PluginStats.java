/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.common.annotation.InternalApi;

/**
 * Marker interface for plugin-provided stats.
 *
 * Any stats object returned by {@link MetricProvider#stats()}
 * must implement this interface. The server module is responsible
 * for wrapping concrete implementations in a {@code Writeable} /
 * {@code ToXContentFragment} adapter for transport serialization
 * and REST rendering.
 */
@InternalApi
public interface PluginStats {
    // Marker interface — no methods, no extends
}
