/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry;

import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Locale;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;

/**
 * Converts {@link org.opensearch.telemetry.tracing.attributes.Attributes} to OTel {@link Attributes}
 */
public final class OTelAttributesConverter {

    /**
     * Constructor.
     */
    private OTelAttributesConverter() {}

    /**
     * Attribute converter.
     * @param attributes attributes
     * @return otel attributes.
     */
    public static Attributes convert(org.opensearch.telemetry.tracing.attributes.Attributes attributes) {
        AttributesBuilder attributesBuilder = Attributes.builder();
        if (attributes != null) {
            attributes.getAttributesMap().forEach((x, y) -> addSpanAttribute(x, y, attributesBuilder));
        }
        return attributesBuilder.build();
    }

    private static void addSpanAttribute(String key, Object value, AttributesBuilder attributesBuilder) {
        switch (value) {
            case Boolean b -> attributesBuilder.put(key, b);
            case Long l -> attributesBuilder.put(key, l);
            case Double v -> attributesBuilder.put(key, v);
            case String s -> attributesBuilder.put(key, s);
            case null, default -> throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Span attribute value %s type not supported", value)
            );
        }
    }

    /**
     * Attribute converter.
     * @param tags attributes
     * @return otel attributes.
     */
    public static Attributes convert(Tags tags) {
        AttributesBuilder attributesBuilder = Attributes.builder();
        if (tags != null) {
            tags.getTagsMap().forEach((x, y) -> addSpanAttribute(x, y, attributesBuilder));
        }
        return attributesBuilder.build();
    }
}
