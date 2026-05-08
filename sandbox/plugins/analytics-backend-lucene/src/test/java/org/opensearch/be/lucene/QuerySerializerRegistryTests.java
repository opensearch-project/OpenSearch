/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link QuerySerializerRegistry} completeness.
 * Validates Requirement 10.1.
 */
public class QuerySerializerRegistryTests extends OpenSearchTestCase {

    /**
     * Verifies that getSerializers() returns exactly 7 entries.
     */
    public void testRegistryContainsExactlySevenEntries() {
        Map<ScalarFunction, DelegatedPredicateSerializer> serializers = QuerySerializerRegistry.getSerializers();
        assertEquals("Registry should contain exactly 7 serializer entries", 7, serializers.size());
    }

    /**
     * Verifies that the key set matches the expected ScalarFunction constants.
     */
    public void testRegistryKeySetMatchesExpectedFunctions() {
        Map<ScalarFunction, DelegatedPredicateSerializer> serializers = QuerySerializerRegistry.getSerializers();

        Set<ScalarFunction> expectedKeys = Set.of(
            ScalarFunction.MATCH,
            ScalarFunction.MATCH_PHRASE,
            ScalarFunction.MATCH_BOOL_PREFIX,
            ScalarFunction.MATCH_PHRASE_PREFIX,
            ScalarFunction.MULTI_MATCH,
            ScalarFunction.QUERY_STRING,
            ScalarFunction.SIMPLE_QUERY_STRING
        );

        assertEquals("Registry keys should match expected full-text functions", expectedKeys, serializers.keySet());
    }

    /**
     * Verifies that all serializer values are non-null (each entry has a valid lambda).
     */
    public void testAllSerializerValuesAreNonNull() {
        Map<ScalarFunction, DelegatedPredicateSerializer> serializers = QuerySerializerRegistry.getSerializers();

        for (Map.Entry<ScalarFunction, DelegatedPredicateSerializer> entry : serializers.entrySet()) {
            assertNotNull("Serializer for " + entry.getKey() + " should not be null", entry.getValue());
        }
    }
}
