/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link FlushInput}.
 */
public class FlushInputTests extends OpenSearchTestCase {

    public void testEmptyHasNoRowIdMapping() {
        assertFalse(FlushInput.EMPTY.hasRowIdMapping());
        assertNull(FlushInput.EMPTY.rowIdMapping());
    }

    public void testNullMappingHasNoRowIdMapping() {
        FlushInput input = new FlushInput((RowIdMapping) null);
        assertFalse(input.hasRowIdMapping());
    }

    public void testValidMappingHasRowIdMapping() {
        long[] oldToNew = { 2, 0, 1 };
        RowIdMapping mapping = new PackedRowIdMapping(oldToNew, true);
        FlushInput input = new FlushInput(mapping);
        assertTrue(input.hasRowIdMapping());
        assertNotNull(input.rowIdMapping());
        assertEquals(3, input.rowIdMapping().size());
        assertEquals(2L, input.rowIdMapping().getNewRowId(0, RowIdMapping.SINGLE_GEN));
        assertEquals(0L, input.rowIdMapping().getNewRowId(1, RowIdMapping.SINGLE_GEN));
        assertEquals(1L, input.rowIdMapping().getNewRowId(2, RowIdMapping.SINGLE_GEN));
    }
}
