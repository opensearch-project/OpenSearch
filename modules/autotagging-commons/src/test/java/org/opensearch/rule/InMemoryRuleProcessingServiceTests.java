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

import static org.opensearch.rule.attribute_extractor.AttributeExtractor.LogicalOperator.OR;

public class InMemoryRuleProcessingServiceTests extends OpenSearchTestCase {
    InMemoryRuleProcessingService sut;

    public void setUp() throws Exception {
        super.setUp();
        AttributeValueStoreFactory attributeValueStoreFactory = new AttributeValueStoreFactory(
            WLMFeatureType.WLM,
            DefaultAttributeValueStore::new
        );
        sut = new InMemoryRuleProcessingService(attributeValueStoreFactory, WLMFeatureType.WLM.getOrderedAttributes());
    }

    public void testAdd() {
        sut.add(getRule(Set.of("test", "change"), "test_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id", label.get());
    }

    public void testRemove() {
        Rule rule = getRule(Set.of("test", "change"), "test_id");
        sut.add(rule);
        sut.remove(rule);

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testAddThenRemoveWithWildcard() {
        Rule rule = getRule(Set.of("test-*"), "test_id");
        sut.add(rule);
        sut.remove(rule);

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test-index"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForExactMatch() {
        sut.add(getRule(Set.of("test1", "change"), "test_id"));
        sut.add(getRule(Set.of("test", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id1", label.get());
    }

    public void testEvaluateLabelForMultiMatch() {
        sut.add(getRule(Set.of("key1", "change"), "test_id"));
        sut.add(getRule(Set.of("key2", "double"), "new_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("key1", "key2"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForNoMatch() {
        sut.add(getRule(Set.of("test1", "change"), "test_id"));
        sut.add(getRule(Set.of("test", "double"), "test_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("dummy_test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForExactMatchWithLongestMatchingPrefixCase() {
        sut.add(getRule(Set.of("test1*", "change"), "test_id"));
        sut.add(getRule(Set.of("test*", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("testing"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id1", label.get());
    }

    public void testEvaluateLabelForExactValueDoesNotPrefixMatch() {
        // A value stored without a trailing wildcard is matched exactly, so a longer request does not match.
        sut.add(getRule(Set.of("test", "change"), "test_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("testing"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForLongestWildcardPrefixWins() {
        // Two wildcard rules both prefix-match the request; the one with the longer matching prefix scores higher and wins.
        sut.add(getRule(Set.of("test*"), "short_prefix"));
        sut.add(getRule(Set.of("testi*"), "long_prefix"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("testing"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("long_prefix", label.get());
    }

    public void testRemoveExactValueLeavesSiblingWildcardValue() {
        // Exact "my-index" and wildcard "my-index*" are stored under distinct keys; removing the exact rule must
        // not remove the wildcard rule (the pre-fix code stripped '*', conflating the two keys).
        Rule exactRule = getRule(Set.of("my-index"), "exact_group");
        Rule wildcardRule = getRule(Set.of("my-index*"), "prefix_group");
        sut.add(exactRule);
        sut.add(wildcardRule);
        sut.remove(exactRule);

        // The wildcard rule still matches both the exact stem and a longer index.
        assertEquals("prefix_group", sut.evaluateLabel(getAttributeExtractors(List.of("my-index"))).orElse(null));
        assertEquals("prefix_group", sut.evaluateLabel(getAttributeExtractors(List.of("my-index-1"))).orElse(null));

        sut.remove(wildcardRule);
        assertFalse(sut.evaluateLabel(getAttributeExtractors(List.of("my-index-1"))).isPresent());
    }

    public void testRemoveKeepsValuesSharedWithOtherRules() {
        // Both rules leave the second attribute unset, so they share its empty value for the same label
        InMemoryRuleProcessingService service = twoAttributeService();
        Rule second = twoAttributeRule("second", "other");
        service.add(twoAttributeRule("first", "test"));
        service.add(second);
        service.remove(second);

        assertEquals(Optional.of("test_id"), service.evaluateLabel(twoAttributeExtractors("test", "anything")));
    }

    public void testRemoveKeepsSameValueOfOtherRules() {
        // Both rules set the same first value for the same label, and differ in the second attribute
        InMemoryRuleProcessingService service = twoAttributeService();
        Rule second = twoAttributeRule("second", "test", "b");
        service.add(twoAttributeRule("first", "test", "a"));
        service.add(second);
        service.remove(second);

        assertEquals(Optional.of("test_id"), service.evaluateLabel(twoAttributeExtractors("test", "a")));
    }

    public void testEvaluateLabelForNoMatchWithLongestMatchingPrefixCase() {
        sut.add(getRule(Set.of("key1", "change"), "test_id"));
        sut.add(getRule(Set.of("key12", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("key"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    private static Rule getRule(Set<String> attributeValues, String label) {
        return new Rule(
            randomAlphaOfLength(5),
            "test description",
            Map.of(TestAttribute.TEST_ATTRIBUTE, attributeValues),
            WLMFeatureType.WLM,
            label,
            "2025-02-24T07:42:10.123456Z"
        );
    }

    private static List<AttributeExtractor<String>> getAttributeExtractors(List<String> extractedAttributes) {
        List<AttributeExtractor<String>> extractors = new ArrayList<>();
        extractors.add(new AttributeExtractor<String>() {
            @Override
            public Attribute getAttribute() {
                return TestAttribute.TEST_ATTRIBUTE;
            }

            @Override
            public Iterable<String> extract() {
                return extractedAttributes;
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return OR;
            }
        });
        return extractors;
    }

    private static final Attribute SECOND_ATTRIBUTE = () -> "second_attribute";

    private static final FeatureType TWO_ATTRIBUTES = new FeatureType() {
        @Override
        public String getName() {
            return "two_attributes";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(TestAttribute.TEST_ATTRIBUTE, 1, SECOND_ATTRIBUTE, 2);
        }
    };

    private static InMemoryRuleProcessingService twoAttributeService() {
        return new InMemoryRuleProcessingService(
            new AttributeValueStoreFactory(TWO_ATTRIBUTES, DefaultAttributeValueStore::new),
            TWO_ATTRIBUTES.getOrderedAttributes()
        );
    }

    private static Rule twoAttributeRule(String id, String value) {
        return new Rule(
            id,
            "test description",
            Map.of(TestAttribute.TEST_ATTRIBUTE, Set.of(value)),
            TWO_ATTRIBUTES,
            "test_id",
            "2025-02-24T07:42:10.123456Z"
        );
    }

    private static Rule twoAttributeRule(String id, String value, String secondValue) {
        return new Rule(
            id,
            "test description",
            Map.of(TestAttribute.TEST_ATTRIBUTE, Set.of(value), SECOND_ATTRIBUTE, Set.of(secondValue)),
            TWO_ATTRIBUTES,
            "test_id",
            "2025-02-24T07:42:10.123456Z"
        );
    }

    private static List<AttributeExtractor<String>> twoAttributeExtractors(String firstValue, String secondValue) {
        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of(firstValue));
        extractors.add(new AttributeExtractor<>() {
            @Override
            public Attribute getAttribute() {
                return SECOND_ATTRIBUTE;
            }

            @Override
            public Iterable<String> extract() {
                return List.of(secondValue);
            }

            @Override
            public LogicalOperator getLogicalOperator() {
                return OR;
            }
        });
        return extractors;
    }

    public enum WLMFeatureType implements FeatureType {
        WLM;

        static {
            AutoTaggingRegistry.registerFeatureType(WLM);
        }

        @Override
        public String getName() {
            return "wlm";
        }

        @Override
        public Map<Attribute, Integer> getOrderedAttributes() {
            return Map.of(TestAttribute.TEST_ATTRIBUTE, 1);
        }
    }

    public enum TestAttribute implements Attribute {
        TEST_ATTRIBUTE("test_attribute"),
        INVALID_ATTRIBUTE("invalid_attribute");

        private final String name;

        TestAttribute(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public void validateAttribute() {}
    }
}
