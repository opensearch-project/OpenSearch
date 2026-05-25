/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

/**
 * Tests for {@link CompositeDataFormat}.
 */
public class CompositeDataFormatTests extends OpenSearchTestCase {

    public void testNameReturnsComposite() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(primary, List.of(primary));
        assertEquals("composite", format.name());
    }

    public void testPriorityReturnsMinValue() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(primary, List.of(primary));
        assertEquals(Long.MIN_VALUE, format.priority());
    }

    public void testGetPrimaryDataformatReturnsPrimary() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        DataFormat secondary = mockFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary, secondary));
        assertSame(primary, composite.getPrimaryDataFormat());
    }

    public void testSupportedFieldsDelegatesToFirstFormat() {
        FieldTypeCapabilities cap1 = new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
        FieldTypeCapabilities cap2 = new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        DataFormat primary = mockFormat("lucene", 1, Set.of(cap1));
        DataFormat secondary = mockFormat("parquet", 2, Set.of(cap2));

        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary, secondary));
        // supportedFields() returns the first format's fields
        assertEquals(Set.of(cap1), composite.supportedFields());
    }

    public void testSupportedFieldsEmptyWhenNoFormats() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of());
        assertEquals(Set.of(), composite.supportedFields());
    }

    public void testGetDataFormatsReturnsAllFormats() {
        DataFormat f1 = mockFormat("lucene", 1, Set.of());
        DataFormat f2 = mockFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(f1, List.of(f1, f2));
        assertEquals(2, composite.getDataFormats().size());
        assertSame(f1, composite.getDataFormats().get(0));
        assertSame(f2, composite.getDataFormats().get(1));
    }

    public void testGetDataFormatsIsUnmodifiable() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary));
        expectThrows(UnsupportedOperationException.class, () -> composite.getDataFormats().add(mockFormat("x", 0, Set.of())));
    }

    public void testConstructorRejectsNullDataFormats() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(primary, null));
    }

    public void testConstructorRejectsNullPrimaryDataformat() {
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(null, List.of()));
    }

    public void testToStringContainsClassName() {
        DataFormat primary = mockFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(primary, List.of(primary));
        String str = composite.toString();
        assertTrue(str.contains("CompositeDataFormat"));
        assertTrue(str.contains("dataFormats="));
    }

    private DataFormat mockFormat(String name, long priority, Set<FieldTypeCapabilities> fields) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return fields;
            }
        };
    }
}
