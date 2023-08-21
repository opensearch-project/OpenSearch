/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.internal.InternalAttributeKeyImpl;

public class OTelAttributesConverterTests extends OpenSearchTestCase {

    public void testConverterNullAttributes() {
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(null);
        assertEquals(0, otelAttributes.size());
    }

    public void testConverterEmptyAttributes() {
        Attributes attributes = Attributes.EMPTY;
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(null);
        assertEquals(0, otelAttributes.size());
    }

    public void testConverterSingleAttributes() {
        Attributes attributes = Attributes.create().addAttribute("key1", "value");
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(attributes);
        assertEquals(1, otelAttributes.size());
        assertEquals("value", otelAttributes.get(InternalAttributeKeyImpl.create("key1", AttributeType.STRING)));
    }

    public void testConverterMultipleAttributes() {
        Attributes attributes = Attributes.create()
            .addAttribute("key1", 1l)
            .addAttribute("key2", 1.0)
            .addAttribute("key3", true)
            .addAttribute("key4", "value4");
        Map<String, ?> attributeMap = attributes.getAttributesMap();
        io.opentelemetry.api.common.Attributes otelAttributes = OTelAttributesConverter.convert(attributes);
        assertEquals(4, otelAttributes.size());
        otelAttributes.asMap().forEach((x, y) -> assertEquals(attributeMap.get(x.getKey()), y));
    }
}
