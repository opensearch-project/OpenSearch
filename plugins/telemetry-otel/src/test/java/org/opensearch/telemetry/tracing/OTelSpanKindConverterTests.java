/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.test.OpenSearchTestCase;

import io.opentelemetry.api.trace.SpanKind;

public class OTelSpanKindConverterTests extends OpenSearchTestCase {

    public void testSpanKindNullConverterNull() {
        assertEquals(SpanKind.INTERNAL, OTelSpanKindConverter.convert(null));
    }

    public void testSpanKindConverter() {
        assertEquals(SpanKind.INTERNAL, OTelSpanKindConverter.convert(org.opensearch.telemetry.tracing.SpanKind.INTERNAL));
        assertEquals(SpanKind.CLIENT, OTelSpanKindConverter.convert(org.opensearch.telemetry.tracing.SpanKind.CLIENT));
        assertEquals(SpanKind.SERVER, OTelSpanKindConverter.convert(org.opensearch.telemetry.tracing.SpanKind.SERVER));
    }

}
