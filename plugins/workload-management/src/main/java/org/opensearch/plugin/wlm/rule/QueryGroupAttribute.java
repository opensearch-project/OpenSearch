/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.autotagging.Attribute;

import java.util.HashMap;
import java.util.Map;

/**
 * Attributes specific to the query group feature.
 * @opensearch.experimental
 */
public enum QueryGroupAttribute implements Attribute {
    /**
     * Represents the index_pattern attribute in QueryGroupAttribute
     */
    INDEX_PATTERN("index_pattern");

    private final String name;

    QueryGroupAttribute(String name) {
        this.name = name;
        validateAttribute();
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Retrieves the QueryGroupAttribute from a name string
     * @param name - attribute name
     */
    public static QueryGroupAttribute fromName(String name) {
        for (QueryGroupAttribute attr : QueryGroupAttribute.values()) {
            if (attr.getName().equals(name)) {
                return attr;
            }
        }
        throw new IllegalArgumentException("Unknown QueryGroupAttribute: " + name);
    }

    /**
     * Converts the QueryGroupAttribute values into a map with attribute names as keys.
     */
    public static Map<String, Attribute> toMap() {
        Map<String, Attribute> map = new HashMap<>();
        for (QueryGroupAttribute attr : QueryGroupAttribute.values()) {
            map.put(attr.getName(), attr);
        }
        return map;
    }
}
