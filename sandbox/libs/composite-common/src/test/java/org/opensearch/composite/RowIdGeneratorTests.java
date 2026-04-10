/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link RowIdGenerator}.
 */
public class RowIdGeneratorTests extends OpenSearchTestCase {

    public void testNextRowIdStartsAtZero() {
        RowIdGenerator generator = new RowIdGenerator("test");
        assertEquals(0L, generator.nextRowId());
    }

    public void testNextRowIdIncrementsMonotonically() {
        RowIdGenerator generator = new RowIdGenerator("test");
        for (int i = 0; i < 100; i++) {
            assertEquals(i, generator.nextRowId());
        }
    }

    public void testCurrentRowIdReturnsCurrentWithoutIncrementing() {
        RowIdGenerator generator = new RowIdGenerator("test");
        assertEquals(0L, generator.currentRowId());
        assertEquals(0L, generator.currentRowId());
        generator.nextRowId();
        assertEquals(1L, generator.currentRowId());
        assertEquals(1L, generator.currentRowId());
    }

    public void testGetSourceReturnsConstructorArgument() {
        String source = randomAlphaOfLength(10);
        RowIdGenerator generator = new RowIdGenerator(source);
        assertEquals(source, generator.getSource());
    }

    public void testCurrentRowIdReflectsNextRowIdCalls() {
        RowIdGenerator generator = new RowIdGenerator("test");
        int count = randomIntBetween(1, 50);
        for (int i = 0; i < count; i++) {
            generator.nextRowId();
        }
        assertEquals(count, generator.currentRowId());
    }
}
