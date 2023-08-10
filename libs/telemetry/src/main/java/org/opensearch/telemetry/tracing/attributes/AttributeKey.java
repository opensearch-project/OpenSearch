/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing.attributes;

/**
 * Attribute class to hold key and value type.
 */
public class AttributeKey {
    private final String key;
    private final AttributeType type;

    /**
     * Constructor.
     * @param key key
     * @param type type
     */
    public AttributeKey(String key, AttributeType type) {
        this.key = key;
        this.type = type;
    }

    /**
     * Returns the key.
     * @return key.
     */
    public String getKey() {
        return key;
    }

    /**
     * Returns attribute type.
     * @return attribute type.
     */
    public AttributeType getType() {
        return type;
    }
}
