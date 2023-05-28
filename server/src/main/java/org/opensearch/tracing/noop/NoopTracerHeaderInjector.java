/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing.noop;

import org.opensearch.tracing.TracerHeaderInjector;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * No-op implementation of TracerHeaderInjector
 */
public class NoopTracerHeaderInjector implements TracerHeaderInjector {

    public static final TracerHeaderInjector INSTANCE = new NoopTracerHeaderInjector();

    private NoopTracerHeaderInjector() {}

    @Override
    public BiConsumer<Map<String, String>, Map<String, Object>> injectHeader() {
        return (x, y) -> {};
    }
}
