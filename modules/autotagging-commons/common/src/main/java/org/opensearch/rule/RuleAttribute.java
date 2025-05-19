/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.Attribute;

/**
 * Generic Rule attributes that features can use out of the use by using the lib.
 * @opensearch.experimental
 */
public enum RuleAttribute implements Attribute {
    /**
     * Represents the index_pattern attribute in RuleAttribute
     */
    INDEX_PATTERN("index_pattern");

    private final String name;

    RuleAttribute(String name) {
        this.name = name;
        validateAttribute();
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Retrieves the RuleAttribute from a name string
     * @param name - attribute name
     */
    public static RuleAttribute fromName(String name) {
        for (RuleAttribute attr : RuleAttribute.values()) {
            if (attr.getName().equals(name)) {
                return attr;
            }
        }
        throw new IllegalArgumentException("Unknown RuleAttribute: " + name);
    }
}
