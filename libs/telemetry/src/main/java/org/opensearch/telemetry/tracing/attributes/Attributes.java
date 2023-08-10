/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.attributes;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to create attributes for a span.
 */
public class Attributes {
    private final Map<AttributeKey, Object> attributesMap;

    /**
     * Factory method.
     * @return attributes.
     */
    public static Attributes create() {
        return new Attributes();
    }

    /**
     * Constructor.
     */
    private Attributes() {
        this.attributesMap = new HashMap<>();
    }

    /**
     * Add String attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, String value) {
        attributesMap.put(new AttributeKey(key, AttributeType.STRING), value);
        return this;
    }

    /**
     * Add long attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, long value) {
        attributesMap.put(new AttributeKey(key, AttributeType.LONG), value);
        return this;
    };

    /**
     * Add double attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, double value) {
        attributesMap.put(new AttributeKey(key, AttributeType.DOUBLE), value);
        return this;
    };

    /**
     * Add boolean attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, boolean value) {
        attributesMap.put(new AttributeKey(key, AttributeType.BOOLEAN), value);
        return this;
    };

    /**
     * Returns the attribute map.
     * @return attributes map
     */
    public Map<AttributeKey, ?> getAttributesMap() {
        return attributesMap;
    }

}
