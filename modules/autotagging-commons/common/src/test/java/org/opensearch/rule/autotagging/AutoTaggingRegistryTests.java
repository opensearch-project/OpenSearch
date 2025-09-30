/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.util.HashMap;

import static org.opensearch.rule.autotagging.AutoTaggingRegistry.MAX_FEATURE_TYPE_NAME_LENGTH;
import static org.opensearch.rule.autotagging.RuleTests.INVALID_FEATURE;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_TYPE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoTaggingRegistryTests extends OpenSearchTestCase {

    @BeforeClass
    public static void setUpOnce() {
        FeatureType featureType = RuleTestUtils.MockRuleFeatureType.INSTANCE;
        AutoTaggingRegistry.registerFeatureType(featureType);
    }

    public void testGetFeatureType_Success() {
        FeatureType retrievedFeatureType = AutoTaggingRegistry.getFeatureType(FEATURE_TYPE_NAME);
        assertEquals(FEATURE_TYPE_NAME, retrievedFeatureType.getName());
    }

    public void testRuntimeException() {
        assertThrows(ResourceNotFoundException.class, () -> AutoTaggingRegistry.getFeatureType(INVALID_FEATURE));
    }

    public void testIllegalStateExceptionException() {
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(null));

        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(FEATURE_TYPE_NAME);
        when(featureType.getOrderedAttributes()).thenReturn(null);
        when(featureType.getFeatureValueValidator()).thenReturn(new FeatureValueValidator() {
            @Override
            public void validate(String featureValue) {}
        });
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));

        when(featureType.getName()).thenReturn(randomAlphaOfLength(MAX_FEATURE_TYPE_NAME_LENGTH + 1));
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));

        when(featureType.getName()).thenReturn(FEATURE_TYPE_NAME);
        when(featureType.getOrderedAttributes()).thenReturn(new HashMap<>());
        when(featureType.getFeatureValueValidator()).thenReturn(null);
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));
    }
}
