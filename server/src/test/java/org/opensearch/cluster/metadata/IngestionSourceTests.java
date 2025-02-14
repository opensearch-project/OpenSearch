/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class IngestionSourceTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource("type", "pointerInitReset", params);

        assertEquals("type", source.getType());
        assertEquals("pointerInitReset", source.getPointerInitReset());
        assertEquals(params, source.params());
    }

    public void testEquals() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource("type", "pointerInitReset", params1);

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource("type", "pointerInitReset", params2);

        assertTrue(source1.equals(source2));
        assertTrue(source2.equals(source1));

        IngestionSource source3 = new IngestionSource("differentType", "pointerInitReset", params1);
        assertFalse(source1.equals(source3));
    }

    public void testHashCode() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource("type", "pointerInitReset", params1);

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource("type", "pointerInitReset", params2);

        assertEquals(source1.hashCode(), source2.hashCode());

        IngestionSource source3 = new IngestionSource("differentType", "pointerInitReset", params1);
        assertNotEquals(source1.hashCode(), source3.hashCode());
    }

    public void testToString() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource("type", "pointerInitReset", params);

        String expected = "IngestionSource{type='type',pointer_init_reset='pointerInitReset', params={key=value}}";
        assertEquals(expected, source.toString());
    }
}
