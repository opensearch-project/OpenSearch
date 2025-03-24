/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.indices.pollingingest.IngestionErrorStrategy.ErrorStrategy.DROP;

public class IngestionSourceTests extends OpenSearchTestCase {

    private final IngestionSource.PointerInitReset pointerInitReset = new IngestionSource.PointerInitReset(
        StreamPoller.ResetState.REWIND_BY_OFFSET,
        "1000"
    );

    public void testConstructorAndGetters() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource("type", pointerInitReset, DROP, params);

        assertEquals("type", source.getType());
        assertEquals(StreamPoller.ResetState.REWIND_BY_OFFSET, source.getPointerInitReset().getType());
        assertEquals("1000", source.getPointerInitReset().getValue());
        assertEquals(DROP, source.getErrorStrategy());
        assertEquals(params, source.params());
    }

    public void testEquals() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource("type", pointerInitReset, DROP, params1);

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource("type", pointerInitReset, DROP, params2);
        assertTrue(source1.equals(source2));
        assertTrue(source2.equals(source1));

        IngestionSource source3 = new IngestionSource("differentType", pointerInitReset, DROP, params1);
        assertFalse(source1.equals(source3));
    }

    public void testHashCode() {
        Map<String, Object> params1 = new HashMap<>();
        params1.put("key", "value");
        IngestionSource source1 = new IngestionSource("type", pointerInitReset, DROP, params1);

        Map<String, Object> params2 = new HashMap<>();
        params2.put("key", "value");
        IngestionSource source2 = new IngestionSource("type", pointerInitReset, DROP, params2);
        assertEquals(source1.hashCode(), source2.hashCode());

        IngestionSource source3 = new IngestionSource("differentType", pointerInitReset, DROP, params1);
        assertNotEquals(source1.hashCode(), source3.hashCode());
    }

    public void testToString() {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        IngestionSource source = new IngestionSource("type", pointerInitReset, DROP, params);
        String expected =
            "IngestionSource{type='type',pointer_init_reset='PointerInitReset{type='REWIND_BY_OFFSET', value=1000}',error_strategy='DROP', params={key=value}}";
        assertEquals(expected, source.toString());
    }
}
