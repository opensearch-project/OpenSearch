/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.trace.SpanKind;

/**
 * Converts {@link org.opensearch.telemetry.tracing.SpanKind} to OTel {@link SpanKind}
 */
final class OTelSpanKindConverter {

    /**
     * Constructor.
     */
    private OTelSpanKindConverter() {}

    /**
     * SpanKind converter.
     * @param spanKind span kind.
     * @return otel attributes.
     */
    static SpanKind convert(org.opensearch.telemetry.tracing.SpanKind spanKind) {
        if (spanKind == null) {
            return SpanKind.INTERNAL;
        } else {
            switch (spanKind) {
                case CLIENT:
                    return SpanKind.CLIENT;
                case SERVER:
                    return SpanKind.SERVER;
                case INTERNAL:
                default:
                    return SpanKind.INTERNAL;
            }
        }
    }
}
