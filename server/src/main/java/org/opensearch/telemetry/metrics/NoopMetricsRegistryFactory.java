/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.telemetry.metrics.noop.NoopMetricsRegistry;

import java.util.Optional;

/**
 * No-op implementation of {@link MetricsRegistryFactory}
 *
 * @opensearch.internal
 */
@InternalApi
public class NoopMetricsRegistryFactory extends MetricsRegistryFactory {
    public NoopMetricsRegistryFactory() {
        super(null, Optional.empty());
    }

    @Override
    public MetricsRegistry getMetricsRegistry() {
        return NoopMetricsRegistry.INSTANCE;
    }

    @Override
    public void close() {

    }
}
