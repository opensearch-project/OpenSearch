/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.attributes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to create attributes for a span.
 */
public class Attributes {
    private final Map<String, Object> attributesMap;
    /**
     * Empty value.
     */
    public final static Attributes EMPTY = new Attributes(Collections.emptyMap());

    /**
     * Factory method.
     * @return attributes.
     */
    public static Attributes create() {
        return new Attributes(new HashMap<>());
    }

    /**
     * Constructor.
     */
    private Attributes(Map<String, Object> attributesMap) {
        this.attributesMap = attributesMap;
    }

    /**
     * Add String attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, String value) {
        Objects.requireNonNull(value, "value cannot be null");
        attributesMap.put(key, value);
        return this;
    }

    /**
     * Add long attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, long value) {
        attributesMap.put(key, value);
        return this;
    };

    /**
     * Add double attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, double value) {
        attributesMap.put(key, value);
        return this;
    };

    /**
     * Add boolean attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Attributes addAttribute(String key, boolean value) {
        attributesMap.put(key, value);
        return this;
    };

    /**
     * Returns the attribute map.
     * @return attributes map
     */
    public Map<String, ?> getAttributesMap() {
        return Collections.unmodifiableMap(attributesMap);
    }

}
