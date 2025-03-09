/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.autotagging.RuleTests.INVALID_ATTRIBUTE;
import static org.opensearch.autotagging.RuleTests.INVALID_CLASS;
import static org.opensearch.autotagging.RuleTests.INVALID_FEATURE;
import static org.opensearch.autotagging.RuleTests.TEST_ATTR1_NAME;
import static org.opensearch.autotagging.RuleTests.TEST_FEATURE_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoTaggingRegistryTests extends OpenSearchTestCase {

    public void testRegisterFeatureType_Success() {
        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(TEST_FEATURE_TYPE);
        AutoTaggingRegistry.registerFeatureType(featureType);
        Tuple<String, String> key = new Tuple<>(featureType.getClass().getName(), TEST_FEATURE_TYPE);
        assertTrue(AutoTaggingRegistry.featureTypesRegistryMap.containsKey(key));
        assertEquals(featureType, AutoTaggingRegistry.featureTypesRegistryMap.get(key));
    }

    public void testRegisterAttribute_Success() {
        Attribute attribute = mock(Attribute.class);
        when(attribute.getName()).thenReturn(TEST_ATTR1_NAME);
        AutoTaggingRegistry.registerAttribute(attribute);
        Tuple<String, String> key = new Tuple<>(attribute.getClass().getName(), TEST_ATTR1_NAME);
        assertTrue(AutoTaggingRegistry.attributeRegistryMap.containsKey(key));
        assertEquals(attribute, AutoTaggingRegistry.attributeRegistryMap.get(key));
    }

    public void testGetFeatureType_Success() {
        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(TEST_FEATURE_TYPE);
        AutoTaggingRegistry.registerFeatureType(featureType);
        FeatureType retrievedFeatureType = AutoTaggingRegistry.getFeatureType(featureType.getClass().getName(), TEST_FEATURE_TYPE);
        assertEquals(featureType, retrievedFeatureType);
    }

    public void testGetAttribute_Success() {
        Attribute attribute = mock(Attribute.class);
        when(attribute.getName()).thenReturn(TEST_ATTR1_NAME);
        AutoTaggingRegistry.registerAttribute(attribute);
        Attribute retrievedAttribute = AutoTaggingRegistry.getAttribute(attribute.getClass().getName(), TEST_ATTR1_NAME);
        assertEquals(attribute, retrievedAttribute);
    }

    public void testRuntimeException() {
        assertThrows(ResourceNotFoundException.class, () -> AutoTaggingRegistry.getFeatureType(INVALID_CLASS, INVALID_FEATURE));
        assertThrows(ResourceNotFoundException.class, () -> AutoTaggingRegistry.getAttribute(INVALID_CLASS, INVALID_ATTRIBUTE));
    }

    public void testIllegalStateExceptionException() {
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerAttribute(null));
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(null));
    }
}
