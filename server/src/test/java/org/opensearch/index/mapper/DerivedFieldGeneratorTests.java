/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DerivedFieldGenerator}
 */
public class DerivedFieldGeneratorTests extends OpenSearchTestCase {

    private MappedFieldType mockFieldType;
    private FieldValueFetcher mockDocValuesFetcher;
    private FieldValueFetcher mockStoredFieldFetcher;
    private LeafReader mockReader;

    @Before
    public void setupTest() {
        mockFieldType = mock(MappedFieldType.class);
        mockDocValuesFetcher = mock(FieldValueFetcher.class);
        mockStoredFieldFetcher = mock(FieldValueFetcher.class);
        mockReader = mock(LeafReader.class);
        when(mockFieldType.name()).thenReturn("test_field");
    }

    public void testConstructorWithDefaultDerivedSourceKeep() {
        when(mockFieldType.hasDocValues()).thenReturn(true);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher
        );

        assertEquals(DerivedSourceKeep.NONE, generator.getDerivedSourceKeep());
    }

    public void testConstructorThrowsOnNullDerivedSourceKeep() {
        when(mockFieldType.hasDocValues()).thenReturn(true);

        NullPointerException exception = expectThrows(NullPointerException.class, () -> {
            new DerivedFieldGenerator(mockFieldType, mockDocValuesFetcher, mockStoredFieldFetcher, null);
        });

        assertTrue(exception.getMessage().contains("derivedSourceKeep cannot be null"));
    }

    public void testConstructorThrowsWhenArraysModeWithoutStoredFields() {
        when(mockFieldType.hasDocValues()).thenReturn(true);

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            new DerivedFieldGenerator(mockFieldType, mockDocValuesFetcher, null, DerivedSourceKeep.ARRAYS);
        });

        assertTrue(exception.getMessage().contains("derived_source_keep='arrays' requires stored fields"));
        assertTrue(exception.getMessage().contains("test_field"));
    }

    public void testUsesDocValuesFetcherWhenAvailable() throws IOException {
        when(mockFieldType.hasDocValues()).thenReturn(true);
        List<Object> values = Collections.singletonList("value1");
        when(mockDocValuesFetcher.fetch(any(LeafReader.class), eq(0))).thenReturn(values);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher,
            DerivedSourceKeep.NONE
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        generator.generate(builder, mockReader, 0);
        builder.endObject();

        verify(mockDocValuesFetcher, times(1)).fetch(mockReader, 0);
        verify(mockDocValuesFetcher, times(1)).write(eq(builder), eq(values));
    }

    public void testUsesStoredFieldFetcherWhenDocValuesNotAvailable() throws IOException {
        when(mockFieldType.hasDocValues()).thenReturn(false);
        List<Object> values = Collections.singletonList("value1");
        when(mockStoredFieldFetcher.fetch(any(LeafReader.class), eq(0))).thenReturn(values);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher,
            DerivedSourceKeep.NONE
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        generator.generate(builder, mockReader, 0);
        builder.endObject();

        verify(mockStoredFieldFetcher, times(1)).fetch(mockReader, 0);
    }

    public void testUsesStoredFieldFetcherWithArraysMode() throws IOException {
        when(mockFieldType.hasDocValues()).thenReturn(true);
        List<Object> values = List.of("value1", "value2");
        when(mockStoredFieldFetcher.fetch(any(LeafReader.class), eq(0))).thenReturn(values);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher,
            DerivedSourceKeep.ARRAYS
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        generator.generate(builder, mockReader, 0);
        builder.endObject();

        verify(mockStoredFieldFetcher, times(1)).fetch(mockReader, 0);
        verify(mockDocValuesFetcher, times(0)).fetch(any(), any(int.class));
    }

    public void testGetDerivedFieldPreferenceWithDocValues() {
        when(mockFieldType.hasDocValues()).thenReturn(true);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher
        );

        assertEquals(FieldValueType.DOC_VALUES, generator.getDerivedFieldPreference());
    }

    public void testGetDerivedFieldPreferenceWithoutDocValues() {
        when(mockFieldType.hasDocValues()).thenReturn(false);

        DerivedFieldGenerator generator = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher
        );

        assertEquals(FieldValueType.STORED, generator.getDerivedFieldPreference());
    }

    public void testGetDerivedSourceKeep() {
        when(mockFieldType.hasDocValues()).thenReturn(true);

        DerivedFieldGenerator generatorNone = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher,
            DerivedSourceKeep.NONE
        );
        assertEquals(DerivedSourceKeep.NONE, generatorNone.getDerivedSourceKeep());

        DerivedFieldGenerator generatorArrays = new DerivedFieldGenerator(
            mockFieldType,
            mockDocValuesFetcher,
            mockStoredFieldFetcher,
            DerivedSourceKeep.ARRAYS
        );
        assertEquals(DerivedSourceKeep.ARRAYS, generatorArrays.getDerivedSourceKeep());
    }

    public void testNullDocValuesFetcherWhenDocValuesPreferred() {
        when(mockFieldType.hasDocValues()).thenReturn(true);
        AssertionError error = expectThrows(AssertionError.class, () -> {
            new DerivedFieldGenerator(
                mockFieldType,
                null,
                mockStoredFieldFetcher,
                DerivedSourceKeep.NONE
            );
        });
        assertNotNull(error);
    }

    public void testNullStoredFieldFetcherWhenStoredFieldsPreferred() {
        when(mockFieldType.hasDocValues()).thenReturn(false);
        AssertionError error = expectThrows(AssertionError.class, () -> {
            new DerivedFieldGenerator(
                mockFieldType,
                mockDocValuesFetcher,
                null,
                DerivedSourceKeep.NONE
            );
        });
        assertNotNull(error);
    }
}
