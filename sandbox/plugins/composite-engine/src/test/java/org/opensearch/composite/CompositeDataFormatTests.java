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
        CompositeDataFormat format = new CompositeDataFormat(List.of(mockFormat("lucene", 1, Set.of())));
        assertEquals("composite", format.name());
    }

    public void testPriorityReturnsMinValue() {
        CompositeDataFormat format = new CompositeDataFormat(List.of(mockFormat("lucene", 1, Set.of())));
        assertEquals(Long.MIN_VALUE, format.priority());
    }

    public void testDefaultConstructorReturnsEmptyFormats() {
        CompositeDataFormat format = new CompositeDataFormat();
        assertTrue(format.getDataFormats().isEmpty());
        assertEquals(Set.of(), format.supportedFields());
    }

    public void testSupportedFieldsDelegatesToFirstFormat() {
        FieldTypeCapabilities cap1 = new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
        FieldTypeCapabilities cap2 = new FieldTypeCapabilities("integer", Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE));
        DataFormat primary = mockFormat("lucene", 1, Set.of(cap1));
        DataFormat secondary = mockFormat("parquet", 2, Set.of(cap2));

        CompositeDataFormat composite = new CompositeDataFormat(List.of(primary, secondary));
        // supportedFields() returns the first format's fields
        assertEquals(Set.of(cap1), composite.supportedFields());
    }

    public void testSupportedFieldsEmptyWhenNoFormats() {
        CompositeDataFormat composite = new CompositeDataFormat(List.of());
        assertEquals(Set.of(), composite.supportedFields());
    }

    public void testGetDataFormatsReturnsAllFormats() {
        DataFormat f1 = mockFormat("lucene", 1, Set.of());
        DataFormat f2 = mockFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(List.of(f1, f2));
        assertEquals(2, composite.getDataFormats().size());
        assertSame(f1, composite.getDataFormats().get(0));
        assertSame(f2, composite.getDataFormats().get(1));
    }

    public void testGetDataFormatsIsUnmodifiable() {
        CompositeDataFormat composite = new CompositeDataFormat(List.of(mockFormat("lucene", 1, Set.of())));
        expectThrows(UnsupportedOperationException.class, () -> composite.getDataFormats().add(mockFormat("x", 0, Set.of())));
    }

    public void testConstructorRejectsNull() {
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(null));
    }

    public void testToStringContainsClassName() {
        CompositeDataFormat composite = new CompositeDataFormat(List.of(mockFormat("lucene", 1, Set.of())));
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
