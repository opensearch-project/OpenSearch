/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for DerivedSourceKeep enum
 */
public class DerivedSourceKeepTests extends OpenSearchTestCase {

    public void testFromString_ValidValues() {
        assertEquals(DerivedSourceKeep.NONE, DerivedSourceKeep.fromString("none"));
        assertEquals(DerivedSourceKeep.NONE, DerivedSourceKeep.fromString("NONE"));
        assertEquals(DerivedSourceKeep.NONE, DerivedSourceKeep.fromString("None"));
        
        assertEquals(DerivedSourceKeep.ARRAYS, DerivedSourceKeep.fromString("arrays"));
        assertEquals(DerivedSourceKeep.ARRAYS, DerivedSourceKeep.fromString("ARRAYS"));
        assertEquals(DerivedSourceKeep.ARRAYS, DerivedSourceKeep.fromString("Arrays"));
    }

    public void testFromString_InvalidValue() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DerivedSourceKeep.fromString("invalid")
        );
        assertTrue(e.getMessage().contains("Invalid value for derived_source_keep"));
        assertTrue(e.getMessage().contains("invalid"));
    }

    public void testFromString_NullValue() {
        assertEquals(DerivedSourceKeep.NONE, DerivedSourceKeep.fromString(null));
    }

    public void testRequiresStoredFields() {
        assertTrue(DerivedSourceKeep.ARRAYS.requiresStoredFields());
        assertFalse(DerivedSourceKeep.NONE.requiresStoredFields());
    }

    public void testGetValue() {
        assertEquals("none", DerivedSourceKeep.NONE.getValue());
        assertEquals("arrays", DerivedSourceKeep.ARRAYS.getValue());
    }

    public void testToString() {
        assertEquals("none", DerivedSourceKeep.NONE.toString());
        assertEquals("arrays", DerivedSourceKeep.ARRAYS.toString());
    }
}