/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tracing;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Otel based header injector. This adds Otel headers to facilitate context propagation.
 */
public class OTelTracerHeaderInjector implements TracerHeaderInjector {
    /**
     * No-args constructor
     */
    public OTelTracerHeaderInjector() {}

    @Override
    public BiConsumer<Map<String, String>, Map<String, Object>> injectHeader() {
        return TracerUtils.addTracerContextToHeader();
    }
}
