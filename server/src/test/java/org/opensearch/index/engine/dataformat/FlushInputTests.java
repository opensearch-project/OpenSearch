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

    public void testEmptyHasNoSortPermutation() {
        assertFalse(FlushInput.EMPTY.hasSortPermutation());
        assertNull(FlushInput.EMPTY.sortPermutation());
    }

    public void testNullPermutationHasNoSortPermutation() {
        FlushInput input = new FlushInput(null);
        assertFalse(input.hasSortPermutation());
    }

    public void testEmptyArrayHasNoSortPermutation() {
        FlushInput input = new FlushInput(new long[][] { new long[0], new long[0] });
        assertFalse(input.hasSortPermutation());
    }

    public void testWrongShapeHasNoSortPermutation() {
        FlushInput input = new FlushInput(new long[][] { new long[] { 1, 2 } });
        assertFalse(input.hasSortPermutation());
    }

    public void testValidPermutationHasSortPermutation() {
        long[] oldIds = { 0, 1, 2 };
        long[] newIds = { 2, 0, 1 };
        FlushInput input = new FlushInput(new long[][] { oldIds, newIds });
        assertTrue(input.hasSortPermutation());
        assertSame(oldIds, input.sortPermutation()[0]);
        assertSame(newIds, input.sortPermutation()[1]);
    }
}
