/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.node;

import org.opensearch.test.OpenSearchTestCase;

public class StarTreeNodeTypeTests extends OpenSearchTestCase {

    public void testStarNodeType() {
        assertEquals("star", StarTreeNodeType.STAR.getName());
        assertEquals((byte) -2, StarTreeNodeType.STAR.getValue());
    }

    public void testNullNodeType() {
        assertEquals("null", StarTreeNodeType.NULL.getName());
        assertEquals((byte) -1, StarTreeNodeType.NULL.getValue());
    }

    public void testDefaultNodeType() {
        assertEquals("default", StarTreeNodeType.DEFAULT.getName());
        assertEquals((byte) 0, StarTreeNodeType.DEFAULT.getValue());
    }

    public void testFromValue() {
        assertEquals(StarTreeNodeType.STAR, StarTreeNodeType.fromValue((byte) -2));
        assertEquals(StarTreeNodeType.NULL, StarTreeNodeType.fromValue((byte) -1));
        assertEquals(StarTreeNodeType.DEFAULT, StarTreeNodeType.fromValue((byte) 0));
    }

    public void testFromValueInvalid() {
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> StarTreeNodeType.fromValue((byte) 1));
        assertEquals("Unrecognized value byte to determine star-tree node type: [1]", exception.getMessage());
    }

    public void testEnumValues() {
        StarTreeNodeType[] values = StarTreeNodeType.values();
        assertEquals(3, values.length);
        assertArrayEquals(new StarTreeNodeType[] { StarTreeNodeType.STAR, StarTreeNodeType.NULL, StarTreeNodeType.DEFAULT }, values);
    }

    public void testEnumValueOf() {
        assertEquals(StarTreeNodeType.STAR, StarTreeNodeType.valueOf("STAR"));
        assertEquals(StarTreeNodeType.NULL, StarTreeNodeType.valueOf("NULL"));
        assertEquals(StarTreeNodeType.DEFAULT, StarTreeNodeType.valueOf("DEFAULT"));
    }

    public void testEnumValueOfInvalid() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> StarTreeNodeType.valueOf("INVALID"));
        assertTrue(exception.getMessage().contains("No enum constant"));
    }

}
