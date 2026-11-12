/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.autotagging;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.rule.utils.RuleTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.rule.autotagging.AutoTaggingRegistry.MAX_FEATURE_TYPE_NAME_LENGTH;
import static org.opensearch.rule.autotagging.RuleTests.INVALID_FEATURE;
import static org.opensearch.rule.utils.RuleTestUtils.FEATURE_TYPE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AutoTaggingRegistryTests extends OpenSearchTestCase {

    public void testIndependentRegistriesResolveTheirOwnInstances() throws Exception {
        AutoTaggingRegistry other = new AutoTaggingRegistry();
        FeatureType localFeature = new FeatureType() {
            public String getName() {
                return FEATURE_TYPE_NAME;
            }

            public Map<Attribute, Integer> getOrderedAttributes() {
                return Map.of();
            }
        };
        var readers = RuleTestUtils.namedWriteableRegistry(localFeature);
        other.registerFeatureType(localFeature);
        assertSame(RuleTestUtils.MockRuleFeatureType.INSTANCE, registry.getFeatureType(FEATURE_TYPE_NAME));
        assertSame(localFeature, other.getFeatureType(FEATURE_TYPE_NAME));
        try (var out = new BytesStreamOutput()) {
            out.writeNamedWriteable(RuleTestUtils.MockRuleFeatureType.INSTANCE);
            try (var raw = out.bytes().streamInput()) {
                assertEquals(FEATURE_TYPE_NAME, raw.readString());
                assertEquals(0, raw.available());
            }
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), readers)) {
                assertSame(localFeature, FeatureType.from(in));
                assertEquals(0, in.available());
            }
        }
    }

    public void testUnknownFeatureIsNotResolvedFromAnotherRegistry() throws Exception {
        AutoTaggingRegistry emptyRegistry = new AutoTaggingRegistry();
        var readers = new NamedWriteableRegistry(List.of());
        try (var out = new BytesStreamOutput()) {
            out.writeNamedWriteable(registry.getFeatureType(FEATURE_TYPE_NAME));
            try (var in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), readers)) {
                assertThrows(IllegalArgumentException.class, () -> FeatureType.from(in));
            }
        }
        assertThrows(ResourceNotFoundException.class, () -> emptyRegistry.getFeatureType(null));
    }

    public void testDuplicateNamesRejectedWithinRegistry() {
        registry.registerFeatureType(RuleTestUtils.MockRuleFeatureType.INSTANCE);
        FeatureType duplicate = mock(FeatureType.class);
        when(duplicate.getName()).thenReturn(FEATURE_TYPE_NAME);
        when(duplicate.getOrderedAttributes()).thenReturn(Map.of());
        when(duplicate.getFeatureValueValidator()).thenReturn(value -> {});
        assertThrows(IllegalStateException.class, () -> registry.registerFeatureType(duplicate));
    }

    private AutoTaggingRegistry registry;

    @Before
    public void initializeRegistry() {
        registry = new AutoTaggingRegistry();
        FeatureType featureType = RuleTestUtils.MockRuleFeatureType.INSTANCE;
        registry.registerFeatureType(featureType);
    }

    public void testGetFeatureType_Success() {
        FeatureType retrievedFeatureType = registry.getFeatureType(FEATURE_TYPE_NAME);
        assertEquals(FEATURE_TYPE_NAME, retrievedFeatureType.getName());
    }

    public void testRuntimeException() {
        assertThrows(ResourceNotFoundException.class, () -> registry.getFeatureType(INVALID_FEATURE));
    }

    public void testIllegalStateExceptionException() {
        assertThrows(IllegalStateException.class, () -> registry.registerFeatureType(null));

        FeatureType featureType = mock(FeatureType.class);
        when(featureType.getName()).thenReturn(FEATURE_TYPE_NAME);
        when(featureType.getOrderedAttributes()).thenReturn(null);
        when(featureType.getFeatureValueValidator()).thenReturn(new FeatureValueValidator() {
            @Override
            public void validate(String featureValue) {}
        });
        assertThrows(IllegalStateException.class, () -> registry.registerFeatureType(featureType));

        when(featureType.getName()).thenReturn(randomAlphaOfLength(MAX_FEATURE_TYPE_NAME_LENGTH + 1));
        assertThrows(IllegalStateException.class, () -> registry.registerFeatureType(featureType));

        when(featureType.getName()).thenReturn(FEATURE_TYPE_NAME);
        when(featureType.getOrderedAttributes()).thenReturn(new HashMap<>());
        when(featureType.getFeatureValueValidator()).thenReturn(null);
        assertThrows(IllegalStateException.class, () -> registry.registerFeatureType(featureType));
    }
}
