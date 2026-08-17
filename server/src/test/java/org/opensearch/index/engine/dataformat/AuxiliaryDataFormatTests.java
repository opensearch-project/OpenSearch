/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class AuxiliaryDataFormatTests extends OpenSearchTestCase {

    private static final MockDataFormat PARENT = new MockDataFormat(
        "columnar",
        100L,
        Set.of(new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)))
    );

    public void testNameCarriesPrefixAndRole() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        assertEquals("aux__columnar__nested", child.name());
        assertTrue(child.isAuxiliary());
        assertTrue(DataFormat.isAuxiliaryFormatName(child.name()));
        assertFalse("the delegate must stay a document format", PARENT.isAuxiliary());
    }

    public void testNameForMatchesConstructedName() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        // Backends declare support by name only, without a handle on the delegate's DataFormat.
        assertEquals(child.name(), AuxiliaryDataFormat.nameFor(PARENT.name(), AuxiliaryDataFormat.NESTED_CHILD_ROLE));
    }

    public void testDelegatesPriorityAndSupportedFields() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        assertSame(PARENT, child.delegate());
        assertEquals(AuxiliaryDataFormat.NESTED_CHILD_ROLE, child.role());
        assertEquals(PARENT.priority(), child.priority());
        assertEquals(PARENT.supportedFields(), child.supportedFields());
    }

    public void testStorageIdentityIsTheDelegates() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        // Logical identity keys the catalog; physical identity names the directory the files are
        // actually in — and with it every store resource keyed by format.
        assertSame("a side table's storage is its delegate's, never its own", PARENT, child.storageFormat());
        assertEquals(PARENT.name(), child.storageName());
        assertNotEquals(child.name(), child.storageName());

        // A document format's two identities coincide.
        assertSame(PARENT, PARENT.storageFormat());
        assertEquals(PARENT.name(), PARENT.storageName());
    }

    public void testStorageNameOfAgreesWithTheInstanceMethod() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        // Recovery and replication rebuild a catalog from names alone, so the name-only resolver has
        // to reach the same directory as a caller holding the DataFormat.
        assertEquals(child.storageName(), DataFormat.storageNameOf(child.name()));
        assertEquals(PARENT.storageName(), DataFormat.storageNameOf(PARENT.name()));
    }

    public void testEqualityIsByNameSoTheChildIsNotItsParent() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);
        AuxiliaryDataFormat sameChild = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        assertEquals("separate instances of the same side table must be interchangeable as map keys", child, sameChild);
        assertEquals(child.hashCode(), sameChild.hashCode());
        assertNotEquals("the side table must never collide with the table it sits beside", child, PARENT);
    }

    public void testRejectsAuxiliaryDelegate() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AuxiliaryDataFormat(child, AuxiliaryDataFormat.NESTED_CHILD_ROLE)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("Cannot nest auxiliary formats"));
    }

    public void testRejectsEmptyRole() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new AuxiliaryDataFormat(PARENT, ""));
        assertTrue(e.getMessage(), e.getMessage().contains("must be non-empty"));
    }

    public void testRejectsRoleContainingSeparator() {
        // Otherwise `aux__columnar__a__b` would be ambiguous to read back.
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AuxiliaryDataFormat(PARENT, "a" + AuxiliaryDataFormat.ROLE_SEPARATOR + "b")
        );
        assertTrue(e.getMessage(), e.getMessage().contains(AuxiliaryDataFormat.ROLE_SEPARATOR));
    }

    public void testRoleOfDecomposesTheNameAlongsideStorageNameOf() {
        AuxiliaryDataFormat child = new AuxiliaryDataFormat(PARENT, AuxiliaryDataFormat.NESTED_CHILD_ROLE);

        // The two halves of the name, each recoverable without the DataFormat instance.
        assertEquals(PARENT.name(), DataFormat.storageNameOf(child.name()));
        assertEquals(AuxiliaryDataFormat.NESTED_CHILD_ROLE, AuxiliaryDataFormat.roleOf(child.name()));

        assertNull("a document format name has no role", AuxiliaryDataFormat.roleOf(PARENT.name()));
        assertNull("a malformed auxiliary name yields no role rather than a wrong one", AuxiliaryDataFormat.roleOf("aux__norole"));
    }

    public void testGenerationOffsetRoundTrips() {
        assertEquals(AuxiliaryDataFormat.GENERATION_OFFSET + 1L, AuxiliaryDataFormat.generationFor(1L));
        assertEquals(1L, AuxiliaryDataFormat.writerGenerationOf(AuxiliaryDataFormat.generationFor(1L)));

        long generation = randomLongBetween(1L, AuxiliaryDataFormat.GENERATION_OFFSET - 1L);
        assertEquals(generation, AuxiliaryDataFormat.writerGenerationOf(AuxiliaryDataFormat.generationFor(generation)));
    }

    public void testAuxiliaryAndDocumentGenerationRangesAreDisjoint() {
        // The merge path tells a side table from its documents by generation alone, so the two
        // ranges must not meet.
        assertFalse(AuxiliaryDataFormat.isAuxiliaryGeneration(1L));
        assertFalse(AuxiliaryDataFormat.isAuxiliaryGeneration(AuxiliaryDataFormat.GENERATION_OFFSET));
        assertTrue(AuxiliaryDataFormat.isAuxiliaryGeneration(AuxiliaryDataFormat.generationFor(1L)));
    }

    public void testGenerationForRejectsNonDocumentGenerations() {
        expectThrows(IllegalArgumentException.class, () -> AuxiliaryDataFormat.generationFor(0L));
        expectThrows(IllegalArgumentException.class, () -> AuxiliaryDataFormat.generationFor(-1L));
        // No side table of a side table: the offset can only be applied once.
        expectThrows(IllegalArgumentException.class, () -> AuxiliaryDataFormat.generationFor(AuxiliaryDataFormat.generationFor(1L)));
    }

    public void testWriterGenerationOfRejectsDocumentGenerations() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AuxiliaryDataFormat.writerGenerationOf(1L));
        assertTrue(e.getMessage(), e.getMessage().contains("Not an auxiliary generation"));
    }
}
