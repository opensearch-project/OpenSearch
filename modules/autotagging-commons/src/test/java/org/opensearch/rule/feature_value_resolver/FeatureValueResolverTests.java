/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.feature_value_resolver;

import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class FeatureValueResolverTests extends OpenSearchTestCase {

    private AttributeValueStoreFactory storeFactory;
    private AttributeExtractor<String> extractor;
    private AttributeValueStore<String, String> store;
    private Attribute attribute;

    public void setUp() throws Exception {
        super.setUp();
        storeFactory = mock(AttributeValueStoreFactory.class);
        extractor = mock(AttributeExtractor.class);
        store = mock(AttributeValueStore.class);
        attribute = mock(Attribute.class);
    }

    public void testResolveWithNoExtractorsReturnsEmptyIntersection() {
        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory);
        FeatureValueResolver.FeatureValueResolutionResult result = resolver.resolve(List.of());
        assertNotNull(result);
        assertTrue(result.resolveLabel().isEmpty());
    }

    public void testResolveSingleExtractorSingleSubfieldSingleValue() {
        when(extractor.getAttribute()).thenReturn(attribute);
        when(attribute.getPrioritizedSubfields()).thenReturn(new TreeMap<>(Map.of(1, "username")));
        when(storeFactory.getAttributeValueStore(attribute)).thenReturn(store);

        when(extractor.extract()).thenReturn(List.of("username|alice"));
        when(store.get("username|alice")).thenReturn(List.of(Set.of("label1")));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory);
        FeatureValueResolver.FeatureValueResolutionResult result = resolver.resolve(List.of(extractor));

        assertNotNull(result);
        Optional<String> resolved = result.resolveLabel();
        assertTrue(resolved.isPresent());
        assertEquals("label1", resolved.get());
    }

    public void testResolveSingleExtractorWithNoSubfieldsDefaultsToEmptyString() {
        when(extractor.getAttribute()).thenReturn(attribute);
        when(attribute.getPrioritizedSubfields()).thenReturn(new TreeMap<>());
        when(storeFactory.getAttributeValueStore(attribute)).thenReturn(store);

        when(extractor.extract()).thenReturn(List.of("|value"));
        when(store.get("|value")).thenReturn(List.of(Set.of("labelX")));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory);
        FeatureValueResolver.FeatureValueResolutionResult result = resolver.resolve(List.of(extractor));

        assertNotNull(result);
        assertTrue(result.resolveLabel().isPresent());
        assertEquals("labelX", result.resolveLabel().get());
    }

    public void testResolveMultipleExtractorsIntersection() {
        AttributeExtractor<String> extractor1 = mock(AttributeExtractor.class);
        AttributeExtractor<String> extractor2 = mock(AttributeExtractor.class);
        Attribute attr1 = mock(Attribute.class);
        Attribute attr2 = mock(Attribute.class);
        AttributeValueStore<String, String> store1 = mock(AttributeValueStore.class);
        AttributeValueStore<String, String> store2 = mock(AttributeValueStore.class);

        when(extractor1.getAttribute()).thenReturn(attr1);
        when(extractor2.getAttribute()).thenReturn(attr2);

        when(attr1.getPrioritizedSubfields()).thenReturn(new TreeMap<>(Map.of(1, "username")));
        when(attr2.getPrioritizedSubfields()).thenReturn(new TreeMap<>(Map.of(1, "role")));

        when(storeFactory.getAttributeValueStore(attr1)).thenReturn(store1);
        when(storeFactory.getAttributeValueStore(attr2)).thenReturn(store2);

        when(extractor1.extract()).thenReturn(List.of("username|alice"));
        when(extractor2.extract()).thenReturn(List.of("role|admin"));

        when(store1.get("username|alice")).thenReturn(List.of(Set.of("label1", "common")));
        when(store2.get("role|admin")).thenReturn(List.of(Set.of("common", "label2")));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory);
        FeatureValueResolver.FeatureValueResolutionResult result = resolver.resolve(List.of(extractor1, extractor2));

        assertNotNull(result);
        Optional<String> resolved = result.resolveLabel();
        assertTrue(resolved.isPresent());
        assertEquals("common", resolved.get());
    }

    public void testResolveWithNoIntersectionReturnsEmpty() {
        AttributeExtractor<String> extractor1 = mock(AttributeExtractor.class);
        AttributeExtractor<String> extractor2 = mock(AttributeExtractor.class);
        Attribute attr1 = mock(Attribute.class);
        Attribute attr2 = mock(Attribute.class);
        AttributeValueStore<String, String> store1 = mock(AttributeValueStore.class);
        AttributeValueStore<String, String> store2 = mock(AttributeValueStore.class);

        when(extractor1.getAttribute()).thenReturn(attr1);
        when(extractor2.getAttribute()).thenReturn(attr2);

        when(attr1.getPrioritizedSubfields()).thenReturn(new TreeMap<>(Map.of(1, "username")));
        when(attr2.getPrioritizedSubfields()).thenReturn(new TreeMap<>(Map.of(1, "role")));

        when(storeFactory.getAttributeValueStore(attr1)).thenReturn(store1);
        when(storeFactory.getAttributeValueStore(attr2)).thenReturn(store2);

        when(extractor1.extract()).thenReturn(List.of("username|alice"));
        when(extractor2.extract()).thenReturn(List.of("role|admin"));

        when(store1.get("username|alice")).thenReturn(List.of(Set.of("label1")));
        when(store2.get("role|admin")).thenReturn(List.of(Set.of("label2")));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory);
        FeatureValueResolver.FeatureValueResolutionResult result = resolver.resolve(List.of(extractor1, extractor2));

        assertNotNull(result);
        assertTrue(result.resolveLabel().isEmpty());
    }
}
