/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.autotagging;

import org.opensearch.test.OpenSearchTestCase;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.RuleTests.ATTRIBUTE_MAP;
import static org.opensearch.autotagging.RuleTests.DESCRIPTION;
import static org.opensearch.autotagging.RuleTests.FEATURE_TYPE;
import static org.opensearch.autotagging.RuleTests.LABEL;
import static org.opensearch.autotagging.RuleTests.UPDATED_AT;
import static org.mockito.Mockito.mock;

public class RuleValidatorTests extends OpenSearchTestCase {

    public void testValidRule() {
        RuleValidator<FeatureType> validator = new RuleValidator<>(DESCRIPTION, ATTRIBUTE_MAP, LABEL, UPDATED_AT, FEATURE_TYPE);
        try {
            validator.validate();
        } catch (Exception e) {
            fail("Expected no exception to be thrown, but got: " + e.getClass().getSimpleName());
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends FeatureType> void validateRule(
        String label,
        T featureType,
        Map<Attribute, Set<String>> attributeMap,
        String updatedAt,
        String description
    ) {
        RuleValidator<T> validator = new RuleValidator<>(description, attributeMap, label, updatedAt, featureType);
        validator.validate();
    }

    public void testInvalidDescription() {
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, ATTRIBUTE_MAP, UPDATED_AT, ""));
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, ATTRIBUTE_MAP, UPDATED_AT, null));
    }

    public void testInvalidUpdateTime() {
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, ATTRIBUTE_MAP, null, DESCRIPTION));
    }

    public void testNullOrEmptyAttributeMap() {
        assertThrows(
            IllegalArgumentException.class,
            () -> validateRule(LABEL, FEATURE_TYPE, new HashMap<>(), Instant.now().toString(), DESCRIPTION)
        );
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, null, Instant.now().toString(), DESCRIPTION));
    }

    public void testInvalidAttributeMap() {
        Map<Attribute, Set<String>> map = new HashMap<>();
        Attribute attribute = mock(Attribute.class);
        map.put(attribute, Set.of(""));
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, map, Instant.now().toString(), DESCRIPTION));

        map.put(attribute, Set.of(randomAlphaOfLength(FEATURE_TYPE.getMaxCharLengthPerAttributeValue() + 1)));
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, map, Instant.now().toString(), DESCRIPTION));

        map.put(attribute, new HashSet<>());
        for (int i = 0; i < FEATURE_TYPE.getMaxNumberOfValuesPerAttribute() + 1; i++) {
            map.get(attribute).add(String.valueOf(i));
        }
        assertThrows(IllegalArgumentException.class, () -> validateRule(LABEL, FEATURE_TYPE, map, Instant.now().toString(), DESCRIPTION));
    }

    public void testInvalidFeature() {
        assertThrows(
            IllegalArgumentException.class,
            () -> validateRule(LABEL, null, new HashMap<>(), Instant.now().toString(), DESCRIPTION)
        );
    }

    public void testInvalidLabel() {
        assertThrows(IllegalArgumentException.class, () -> validateRule(null, FEATURE_TYPE, ATTRIBUTE_MAP, UPDATED_AT, DESCRIPTION));
        assertThrows(IllegalArgumentException.class, () -> validateRule("", FEATURE_TYPE, ATTRIBUTE_MAP, UPDATED_AT, DESCRIPTION));
    }
}
