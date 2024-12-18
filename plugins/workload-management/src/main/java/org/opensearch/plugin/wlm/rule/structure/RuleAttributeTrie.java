/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.structure;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RuleAttributeTrie implements FastPrefixMatchingStructure {
    private static final Pattern ALLOWED_ATTRIBUTE_VALUES = Pattern.compile("^[a-zA-Z0-9-_]+\\*?$");
    @Override
    public void add(String s) {

    }

    public enum RuleAttributeName {
        USERNAME("username"),
        INDEX_PATTERN("index_pattern");
        private final String name;

        RuleAttributeName(String name) {
            this.name = name;
        }

        public String getName() { return name; }

        public static RuleAttributeName fromString(String name) {
            for (RuleAttributeName attributeName : RuleAttributeName.values()) {
                if (attributeName.getName().equals(name)) {
                    return attributeName;
                }
            }
            throw new IllegalArgumentException("Invalid rule attribute name [" + name + "]");
        }

    }

    public static class LabeledNode {
        private String label;
        private Map<String, LabeledNode> children = new HashMap<>();

        public LabeledNode() {}

        public LabeledNode(String label) {
            this.label = label;
        }
    }
}
