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
        // mapping[0]=2, mapping[1]=0, mapping[2]=1 means row 0 goes to pos 2, row 1 to pos 0, row 2 to pos 1
        long[] oldToNew = { 2, 0, 1 };
        RowIdMapping mapping = new PackedSingleGenRowIdMapping(oldToNew);
        FlushInput input = new FlushInput(mapping);
        assertTrue(input.hasRowIdMapping());
        assertNotNull(input.rowIdMapping());
        assertEquals(3, input.rowIdMapping().size());
        assertEquals(2, input.rowIdMapping().oldToNew(0));
        assertEquals(0, input.rowIdMapping().oldToNew(1));
        assertEquals(1, input.rowIdMapping().oldToNew(2));
    }
}
