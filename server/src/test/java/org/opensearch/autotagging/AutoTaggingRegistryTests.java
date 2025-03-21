/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import static org.opensearch.autotagging.AutoTaggingRegistry.MAX_FEATURE_TYPE_NAME_LENGTH;
import static org.opensearch.autotagging.RuleTests.INVALID_FEATURE;
import static org.opensearch.autotagging.RuleTests.TEST_FEATURE_TYPE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoTaggingRegistryTests extends OpenSearchTestCase {

    @BeforeClass
    public static void setUpOnce() {
        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(TEST_FEATURE_TYPE);
        AutoTaggingRegistry.registerFeatureType(featureType);
    }

    public void testGetFeatureType_Success() {
        FeatureType retrievedFeatureType = AutoTaggingRegistry.getFeatureType(TEST_FEATURE_TYPE);
        assertEquals(TEST_FEATURE_TYPE, retrievedFeatureType.getName());
    }

    public void testRuntimeException() {
        assertThrows(ResourceNotFoundException.class, () -> AutoTaggingRegistry.getFeatureType(INVALID_FEATURE));
    }

    public void testIllegalStateExceptionException() {
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(null));
        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(TEST_FEATURE_TYPE);
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));
        when(featureType.getName()).thenReturn(randomAlphaOfLength(MAX_FEATURE_TYPE_NAME_LENGTH + 1));
        assertThrows(IllegalStateException.class, () -> AutoTaggingRegistry.registerFeatureType(featureType));
    }
}
