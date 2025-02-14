/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule;

import org.opensearch.plugin.wlm.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryRuleProcessingServiceTests extends OpenSearchTestCase {
    InMemoryRuleProcessingService sut;

    public void setUp() throws Exception {
        super.setUp();
        sut = new InMemoryRuleProcessingService();
    }

    public void testAdd() {
        sut.add(getRule(List.of("test", "change"), "test_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id", label.get());
    }

    public void testRemove() {
        Rule rule = getRule(List.of("test", "change"), "test_id");
        sut.add(rule);
        sut.remove(rule);

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForExactMatch() {
        sut.add(getRule(List.of("test1", "change"), "test_id"));
        sut.add(getRule(List.of("test", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id1", label.get());
    }

    public void testEvaluateLabelForMultiMatch() {
        sut.add(getRule(List.of("key1", "change"), "test_id"));
        sut.add(getRule(List.of("key2", "double"), "new_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("key1", "key2"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForNoMatch() {
        sut.add(getRule(List.of("test1", "change"), "test_id"));
        sut.add(getRule(List.of("test", "double"), "test_id"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("dummy_test"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    public void testEvaluateLabelForExactMatchWithLongestMatchingPrefixCase() {
        sut.add(getRule(List.of("test1", "change"), "test_id"));
        sut.add(getRule(List.of("test", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("testing"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertTrue(label.isPresent());
        assertEquals("test_id1", label.get());
    }

    public void testEvaluateLabelForNoMatchWithLongestMatchingPrefixCase() {
        sut.add(getRule(List.of("key1", "change"), "test_id"));
        sut.add(getRule(List.of("key12", "double"), "test_id1"));

        List<AttributeExtractor<String>> extractors = getAttributeExtractors(List.of("key"));
        Optional<String> label = sut.evaluateLabel(extractors);
        assertFalse(label.isPresent());
    }

    private static Rule getRule(List<String> attributeValues, String label) {
        return new Rule(Map.of(Rule.Attribute.STUB_ATTRIBUTE, attributeValues), Rule.Feature.WLM, label, 123L);
    }

    private static List<AttributeExtractor<String>> getAttributeExtractors(List<String> extractedAttributes) {
        List<AttributeExtractor<String>> extractors = List.of(new AttributeExtractor<String>() {
            @Override
            public Rule.Attribute getAttribute() {
                return Rule.Attribute.STUB_ATTRIBUTE;
            }

            @Override
            public Iterable<String> extract() {
                return extractedAttributes;
            }
        });
        return extractors;
    }
}
