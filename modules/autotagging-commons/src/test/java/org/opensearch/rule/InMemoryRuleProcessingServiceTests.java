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
        sut.add(getRule(Set.of("test1", "change"), "test_id"));
        sut.add(getRule(Set.of("test", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("testing"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id1", label.get());
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
