/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.dsl.converter.ConversionException;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class SimpleFieldGroupingTests {

    private RelDataTypeFactory typeFactory;
    private RelDataType rowType;

    @Before
    public void setUp() {
        typeFactory = mock(RelDataTypeFactory.class);
        rowType = mock(RelDataType.class);
    }

    @Test
    public void testGetFieldNames() {
        SimpleFieldGrouping grouping = new SimpleFieldGrouping(List.of("field1", "field2"));
        assertEquals(List.of("field1", "field2"), grouping.getFieldNames());
    }

    @Test
    public void testResolveIndicesSingleField() throws ConversionException {
        var field = mock(org.apache.calcite.rel.type.RelDataTypeField.class);
        when(field.getIndex()).thenReturn(0);
        when(rowType.getField("name", true, false)).thenReturn(field);

        SimpleFieldGrouping grouping = new SimpleFieldGrouping(List.of("name"));
        List<Integer> indices = grouping.resolveIndices(rowType);

        assertEquals(List.of(0), indices);
    }

    @Test
    public void testResolveIndicesMultipleFields() throws ConversionException {
        var field1 = mock(org.apache.calcite.rel.type.RelDataTypeField.class);
        var field2 = mock(org.apache.calcite.rel.type.RelDataTypeField.class);
        when(field1.getIndex()).thenReturn(1);
        when(field2.getIndex()).thenReturn(3);
        when(rowType.getField("category", true, false)).thenReturn(field1);
        when(rowType.getField("status", true, false)).thenReturn(field2);

        SimpleFieldGrouping grouping = new SimpleFieldGrouping(List.of("category", "status"));
        List<Integer> indices = grouping.resolveIndices(rowType);

        assertEquals(List.of(1, 3), indices);
    }

    @Test
    public void testResolveIndicesFieldNotFound() {
        when(rowType.getField("missing", true, false)).thenReturn(null);

        SimpleFieldGrouping grouping = new SimpleFieldGrouping(List.of("missing"));

        ConversionException ex = assertThrows(ConversionException.class, () -> grouping.resolveIndices(rowType));
        assertTrue(ex.getMessage().contains("missing"));
        assertTrue(ex.getMessage().contains("not found"));
    }
}
