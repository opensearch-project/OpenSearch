/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.apache.commons.collections4.trie.PatriciaTrie;
import org.opensearch.plugin.wlm.rule.storage.AttributeValueStore;
import org.opensearch.plugin.wlm.rule.storage.TrieBasedStore;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * It is a stub which will be removed once Ruirui's PR for Rule schema is merged
 */
public class Rule {
    Map<Attribute, List<String>> attributeMap;
    Feature feature;
    String label;
    long updatedAt;

    /**
     * Test constructor
     * @param attributeMap
     * @param feature
     * @param label
     * @param updatedAt
     */
    public Rule(Map<Attribute, List<String>> attributeMap, Feature feature, String label, long updatedAt) {
        this.attributeMap = attributeMap;
        this.feature = feature;
        this.label = label;
        this.updatedAt = updatedAt;
    }

    /**
     * Stub enum
     */
    public enum Feature {
        /**
         * WLM Feature
         */
        WLM(Set.of(Attribute.STUB_ATTRIBUTE));

        private final Set<Attribute> allowedAttributes;

        Feature(Set<Attribute> allowedAttributes) {
            this.allowedAttributes = allowedAttributes;
        }
    }

    /**
     * Stub Attribute
     */
    public enum Attribute {
        /**
         * Stub attribute
         */
        STUB_ATTRIBUTE(new TrieBasedStore(new PatriciaTrie<>()));

        private final AttributeValueStore valueStore;

        Attribute(AttributeValueStore valueStore) {
            this.valueStore = valueStore;
        }

        /**
         * attribute value store getter
         * @return
         */
        public AttributeValueStore getValueStore() {
            return valueStore;
        }
    }

    /**
     * stub getter
     * @return
     */
    public Feature getFeature() {
        return feature;
    }

    /**
     * stub getter
     * @return
     */
    public Map<Attribute, List<String>> getAttributeMap() {
        return attributeMap;
    }

    /**
     * stub getter
     * @return
     */
    public String getLabel() {
        return label;
    }
}
