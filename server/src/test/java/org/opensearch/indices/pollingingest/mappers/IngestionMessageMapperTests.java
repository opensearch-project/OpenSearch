/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest.mappers;

import org.opensearch.core.Assertions;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class IngestionMessageMapperTests extends OpenSearchTestCase {

    public void test() {
        @SuppressWarnings("unchecked")
        Map raw = new HashMap();
        Map<String, Object> typed = (Map<String, Object>) raw;
        assertNotNull(typed);
    }

    public void testAssertions() {
        try {
            assert false;
            throw new RuntimeException("Assertions not enabled!");
        } catch (AssertionError e) {
            // do nothing
        }
    }

    public void testAssertionStaticVariable() {
        assertTrue(Assertions.ENABLED);
    }
}
