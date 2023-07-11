/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.noop.NoopTracer;

import java.util.Optional;

/**
 * No-op implementation of TracerFactory
 *
 * @opensearch.internal
 */
public class NoopTracerFactory extends TracerFactory {
    public NoopTracerFactory() {
        super(null, Optional.empty(), null);
    }

    @Override
    public Tracer getTracer() {
        return NoopTracer.INSTANCE;
    }

    @Override
    public void close() {

    }

}
