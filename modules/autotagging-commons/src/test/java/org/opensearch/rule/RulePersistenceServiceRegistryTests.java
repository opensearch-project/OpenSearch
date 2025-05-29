/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule;

import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RulePersistenceServiceRegistryTests extends OpenSearchTestCase {
    RulePersistenceServiceRegistry registry = new RulePersistenceServiceRegistry();
    FeatureType mockFeatureType = mock(FeatureType.class);
    RulePersistenceService mockService = mock(RulePersistenceService.class);

    public void testRegisterAndGetService() {
        when(mockFeatureType.getName()).thenReturn("test_feature");
        registry.register(mockFeatureType, mockService);
        RulePersistenceService retrievedService = registry.getRulePersistenceService(mockFeatureType);
        assertSame(mockService, retrievedService);
    }

    public void testRegisterDuplicateService() {
        when(mockFeatureType.getName()).thenReturn("duplicate_feature");
        registry.register(mockFeatureType, mockService);
        RulePersistenceService anotherService = mock(RulePersistenceService.class);
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> registry.register(mockFeatureType, anotherService)
        );
        assertTrue(ex.getMessage().contains("Duplicate rule persistence service: duplicate_feature"));
    }

    public void testGetRulePersistenceService_UnknownFeature() {
        when(mockFeatureType.getName()).thenReturn("unknown_feature");
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> registry.getRulePersistenceService(mockFeatureType)
        );
        assertTrue(ex.getMessage().contains("Unknown feature type"));
    }
}
