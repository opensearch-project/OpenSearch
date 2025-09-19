/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class FeatureValueCollectorTests extends OpenSearchTestCase {

    private AttributeValueStore<String, String> attributeValueStore;
    private AttributeExtractor<String> attributeExtractor;

    public void setUp() throws Exception {
        super.setUp();
        attributeValueStore = mock(AttributeValueStore.class);
        attributeExtractor = mock(AttributeExtractor.class);
    }

    public void testNoValuesExtractedReturnsNull() {
        when(attributeExtractor.extract()).thenReturn(List.of());

        FeatureValueCollector collector =
            new FeatureValueCollector(attributeValueStore, attributeExtractor, "username");

        CandidateFeatureValues result = collector.collect();

        assertNull(result);
        verifyNoInteractions(attributeValueStore);
    }

    public void testSingleMatchingValueReturnsCandidateValues() {
        when(attributeExtractor.extract()).thenReturn(List.of("username|alice"));
        when(attributeValueStore.get("username|alice")).thenReturn(List.of(Set.of("label1", "label2")));

        FeatureValueCollector collector =
            new FeatureValueCollector(attributeValueStore, attributeExtractor, "username");

        CandidateFeatureValues result = collector.collect();

        assertNotNull(result);
        assertEquals(1, result.getFeatureValuesBySpecificity().size());
        assertTrue(result.getFeatureValuesBySpecificity().get(0).contains("label1"));
        assertTrue(result.getFeatureValuesBySpecificity().get(0).contains("label2"));
    }

    public void testNonMatchingValuesAreIgnored() {
        when(attributeExtractor.extract()).thenReturn(List.of("role|admin"));

        FeatureValueCollector collector =
            new FeatureValueCollector(attributeValueStore, attributeExtractor, "username");

        CandidateFeatureValues result = collector.collect();

        assertNull(result);
        verify(attributeValueStore, never()).get(any());
    }

    public void testMultipleMatchingValuesMerged() {
        when(attributeExtractor.extract()).thenReturn(List.of("username|alice", "username|bob"));
        when(attributeValueStore.get("username|alice")).thenReturn(List.of(Set.of("label1")));
        when(attributeValueStore.get("username|bob")).thenReturn(List.of(Set.of("label2")));
        when(attributeExtractor.getLogicalOperator()).thenReturn(AttributeExtractor.LogicalOperator.OR);

        FeatureValueCollector collector =
            new FeatureValueCollector(attributeValueStore, attributeExtractor, "username");

        CandidateFeatureValues result = collector.collect();

        assertNotNull(result);
        assertTrue(result.getFeatureValuesBySpecificity().stream().anyMatch(set -> set.contains("label1") || set.contains("label2")));
    }
}
