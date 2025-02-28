/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SortedNumericDocValuesFetcherTests {
    private MappedFieldType mappedFieldType;
    private LeafReader leafReader;
    private SortedNumericDocValues sortedNumericDocValues;

    private SortedNumericDocValuesFetcher fetcher;

    @Before
    public void setUp() {
        mappedFieldType = mock(MappedFieldType.class);
        leafReader = mock(LeafReader.class);
        sortedNumericDocValues = mock(SortedNumericDocValues.class);
        fetcher = new SortedNumericDocValuesFetcher(mappedFieldType);
    }

    @Test
    public void testFetchSingleValue() throws IOException {
        int docId = 1;
        long expectedValue = 123L;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(1);
        when(sortedNumericDocValues.nextValue()).thenReturn(expectedValue);

        fetcher.fetch(leafReader, docId);

        Assert.assertTrue(fetcher.hasValue());
        assertEquals(1, fetcher.values.size());
        assertEquals(expectedValue, fetcher.values.getFirst());
    }

    @Test
    public void testFetchMultipleValues() throws IOException {
        int docId = 1;
        long[] expectedValues = {1L, 2L, 3L};
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(expectedValues.length);
        when(sortedNumericDocValues.nextValue())
            .thenReturn(expectedValues[0])
            .thenReturn(expectedValues[1])
            .thenReturn(expectedValues[2]);

        fetcher.fetch(leafReader, docId);

        Assert.assertTrue(fetcher.hasValue());
        assertEquals(expectedValues.length, fetcher.values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], fetcher.values.get(i));
        }
    }

    @Test
    public void testFetchNoValues() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(0);

        fetcher.fetch(leafReader, docId);

        Assert.assertFalse(fetcher.hasValue());
        Assert.assertTrue(fetcher.values.isEmpty());
    }

    @Test
    public void testConvert() {
        Long value = 123L;
        String expectedDisplayValue = "123";
        when(mappedFieldType.valueForDisplay(value)).thenReturn(expectedDisplayValue);

        Object result = fetcher.convert(value);

        assertEquals(expectedDisplayValue, result);
        verify(mappedFieldType).valueForDisplay(value);
    }

    @Test
    public void testWriteSingleValue() throws IOException {
        Long value = 123L;
        fetcher.values.add(value);
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(value)).thenReturn(value);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder);
        builder.endObject();

        String expected = "{\"test_field\":123}";
        assertEquals(expected, builder.toString());
    }

    @Test
    public void testWriteMultipleValues() throws IOException {
        Long[] values = {1L, 2L, 3L};
        fetcher.values.addAll(Arrays.asList(values));
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(anyLong())).thenReturn(1, 2, 3);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder);
        builder.endObject();

        String expected = "{\"test_field\":[1,2,3]}";
        assertEquals(expected, builder.toString());
    }

    @Test
    public void testClear() {
        fetcher.values.add(1L);
        fetcher.values.add(2L);
        Assert.assertTrue(fetcher.hasValue());

        fetcher.clear();

        Assert.assertFalse(fetcher.hasValue());
        Assert.assertTrue(fetcher.values.isEmpty());
    }

    @Test
    public void testFetchThrowsIOException() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenThrow(new IOException("Test exception"));

        assertThrows(IOException.class, () -> fetcher.fetch(leafReader, docId));
    }
}
