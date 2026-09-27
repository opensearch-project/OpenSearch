/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.autotagging.AutoTaggingRegistry;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.rule.storage.DefaultAttributeValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.rule.attribute_extractor.AttributeExtractor.LogicalOperator.AND;

/**
 * Tests that an index_pattern rule matches by prefix only when its value ends in a wildcard,
 * and matches exactly otherwise.
 */
public class IndexPatternImplicitWildcardVerificationTests extends OpenSearchTestCase {

    public enum IndexFeatureType implements FeatureType {
        INSTANCE;

        static {
            AutoTaggingRegistry.registerFeatureType(INSTANCE);
        }

        @Override
        public String getName() {
            return "index_verification_feature";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(RuleAttribute.INDEX_PATTERN, 1);
        }
    }

    private InMemoryRuleProcessingService newService() {
        AttributeValueStoreFactory factory = new AttributeValueStoreFactory(IndexFeatureType.INSTANCE, DefaultAttributeValueStore::new);
        return new InMemoryRuleProcessingService(factory, IndexFeatureType.INSTANCE.getOrderedAttributes());
    }

    private Rule indexRule(String indexPattern, String group) {
        return new Rule(
            "rule_" + group,
            "rule for " + indexPattern,
            Map.of(RuleAttribute.INDEX_PATTERN, Set.of(indexPattern)),
            IndexFeatureType.INSTANCE,
            group,
            "2025-02-24T07:42:10.123456Z"
        );
    }

    private Optional<String> evaluateForIndex(InMemoryRuleProcessingService sut, String requestIndex) {
        List<AttributeExtractor<String>> extractors = new ArrayList<>();
        extractors.add(new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return RuleAttribute.INDEX_PATTERN;
            }

            @Override
            public Iterable<String> extract() {
                return List.of(requestIndex);
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return AND;
            }
        });
        return sut.evaluateLabel(extractors);
    }

    public void testExactIndexPatternDoesNotMatchLongerRequestIndex() {
        InMemoryRuleProcessingService sut = newService();
        sut.add(indexRule("my-index", "groupA"));

        // Exact rule "my-index" must NOT match a longer index that merely shares the prefix.
        assertFalse(evaluateForIndex(sut, "my-index-1").isPresent());
    }

    public void testExactIndexPatternMatchesExactRequestIndex() {
        InMemoryRuleProcessingService sut = newService();
        sut.add(indexRule("my-index", "groupA"));

        Optional<String> label = evaluateForIndex(sut, "my-index");
        assertTrue(label.isPresent());
        assertEquals("groupA", label.get());
    }

    public void testWildcardIndexPatternMatchesByPrefix() {
        InMemoryRuleProcessingService sut = newService();
        sut.add(indexRule("my-index*", "groupA"));

        Optional<String> exact = evaluateForIndex(sut, "my-index");
        assertTrue(exact.isPresent());
        assertEquals("groupA", exact.get());

        Optional<String> prefixed = evaluateForIndex(sut, "my-index-1");
        assertTrue(prefixed.isPresent());
        assertEquals("groupA", prefixed.get());
    }

    public void testExactRuleWinsOverShorterPrefixRule() {
        InMemoryRuleProcessingService sut = newService();
        sut.add(indexRule("my-index", "groupExact"));
        sut.add(indexRule("my*", "groupPrefix"));

        // Request "my-index" matches the exact rule at score 1.0 and the shorter prefix "my*" at a lower
        // score, so the exact rule wins.
        Optional<String> exact = evaluateForIndex(sut, "my-index");
        assertTrue(exact.isPresent());
        assertEquals("groupExact", exact.get());
    }

    public void testExactRuleAndEqualStemWildcardRuleTie() {
        InMemoryRuleProcessingService sut = newService();
        sut.add(indexRule("my-index", "groupExact"));
        sut.add(indexRule("my-index*", "groupPrefix"));

        // Request "my-index" matches the exact rule (1.0) and the wildcard rule "my-index*" whose stem
        // equals the request (also 1.0). The scores tie and cannot be broken, so no group is assigned.
        assertFalse(evaluateForIndex(sut, "my-index").isPresent());

        // A longer index matches only the wildcard rule.
        Optional<String> longer = evaluateForIndex(sut, "my-index-1");
        assertTrue(longer.isPresent());
        assertEquals("groupPrefix", longer.get());
    }
}
