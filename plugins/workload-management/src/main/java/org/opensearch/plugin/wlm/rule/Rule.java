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

    public enum Feature {
        WLM(Set.of(Attribute.STUB_ATTRIBUTE));
        private final Set<Attribute> allowedAttributes;

        Feature(Set<Attribute> allowedAttributes) {
            this.allowedAttributes = allowedAttributes;
        }
    }

    public enum Attribute {
        STUB_ATTRIBUTE(new TrieBasedStore(new PatriciaTrie<>()));
        private final AttributeValueStore valueStore;

        Attribute(AttributeValueStore valueStore) {
            this.valueStore = valueStore;
        }

        public AttributeValueStore getValueStore() {
            return valueStore;
        }
    }

    public Feature getFeature() {
        return feature;
    }

    public long getUpdatedAt() {
        return updatedAt;
    }

    public Map<Attribute, List<String>> getAttributeMap() {
        return attributeMap;
    }

    public String getLabel() {
        return label;
    }
}
