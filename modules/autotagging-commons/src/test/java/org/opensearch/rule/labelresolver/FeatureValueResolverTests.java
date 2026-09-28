/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.labelresolver;

import org.opensearch.rule.MatchLabel;
import org.opensearch.rule.attribute_extractor.AttributeExtractor;
import org.opensearch.rule.autotagging.Attribute;
import org.opensearch.rule.storage.AttributeValueStore;
import org.opensearch.rule.storage.AttributeValueStoreFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class FeatureValueResolverTests extends OpenSearchTestCase {

    private AttributeValueStoreFactory storeFactory;
    private AttributeValueStore<String, String> store1;
    private AttributeValueStore<String, String> store2;
    private AttributeExtractor<String> extractor1;
    private AttributeExtractor<String> extractor2;
    private Attribute attr1;
    private Attribute attr2;

    public void setUp() throws Exception {
        super.setUp();
        storeFactory = mock(AttributeValueStoreFactory.class);
        store1 = mock(AttributeValueStore.class);
        store2 = mock(AttributeValueStore.class);
        extractor1 = mock(AttributeExtractor.class);
        extractor2 = mock(AttributeExtractor.class);
        attr1 = mock(Attribute.class);
        attr2 = mock(Attribute.class);

        when(extractor1.getAttribute()).thenReturn(attr1);
        when(extractor2.getAttribute()).thenReturn(attr2);
        when(storeFactory.getAttributeValueStore(attr1)).thenReturn(store1);
        when(storeFactory.getAttributeValueStore(attr2)).thenReturn(store2);
    }

    public void testResolveSingleIntersection() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(
            Arrays.asList(new MatchLabel<>("a", 0.9f), new MatchLabel<>("b", 0.8f))
        );
        when(attr2.findAttributeMatches(extractor2, store2)).thenReturn(
            Arrays.asList(new MatchLabel<>("a", 0.95f), new MatchLabel<>("c", 0.7f))
        );

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Arrays.asList(extractor1, extractor2));
        Optional<String> result = resolver.resolve();
        assertTrue(result.isPresent());
        assertEquals("a", result.get());
    }

    public void testResolveEmptyIntersection() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(Arrays.asList(new MatchLabel<>("a", 0.9f)));
        when(attr2.findAttributeMatches(extractor2, store2)).thenReturn(Arrays.asList(new MatchLabel<>("b", 0.8f)));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Arrays.asList(extractor1, extractor2));
        Optional<String> result = resolver.resolve();
        assertFalse(result.isPresent());
    }

    public void testResolveTieBreakingWithoutResult() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(
            Arrays.asList(new MatchLabel<>("a", 0.9f), new MatchLabel<>("b", 0.9f), new MatchLabel<>("c", 0.7f))
        );
        when(attr2.findAttributeMatches(extractor2, store2)).thenReturn(
            Arrays.asList(new MatchLabel<>("a", 0.95f), new MatchLabel<>("b", 0.95f))
        );

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Arrays.asList(extractor1, extractor2));
        Optional<String> result = resolver.resolve();
        assertFalse(result.isPresent());
    }

    public void testResolveTieBreaking() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(
            Arrays.asList(new MatchLabel<>("a", 0.9f), new MatchLabel<>("b", 0.9f), new MatchLabel<>("c", 0.7f))
        );
        when(attr2.findAttributeMatches(extractor2, store2)).thenReturn(
            Arrays.asList(new MatchLabel<>("b", 0.95f), new MatchLabel<>("a", 0.9f))
        );

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Arrays.asList(extractor1, extractor2));
        Optional<String> result = resolver.resolve();
        assertTrue(result.isPresent());
        assertEquals("b", result.get());
    }

    public void testResolveSingleExtractor() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(Arrays.asList(new MatchLabel<>("x", 1.0f)));

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Collections.singletonList(extractor1));
        Optional<String> result = resolver.resolve();
        assertTrue(result.isPresent());
        assertEquals("x", result.get());
    }

    public void testResolveEmptyFeatureValues() {
        when(attr1.findAttributeMatches(extractor1, store1)).thenReturn(Collections.emptyList());

        FeatureValueResolver resolver = new FeatureValueResolver(storeFactory, Collections.singletonList(extractor1));
        Optional<String> result = resolver.resolve();
        assertFalse(result.isPresent());
    }
}
