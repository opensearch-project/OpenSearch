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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SortedNumericDocValuesFetcherTests extends OpenSearchTestCase {
    private MappedFieldType mappedFieldType;
    private LeafReader leafReader;
    private SortedNumericDocValues sortedNumericDocValues;

    private SortedNumericDocValuesFetcher fetcher;

    @Before
    public void setupTest() {
        mappedFieldType = mock(MappedFieldType.class);
        leafReader = mock(LeafReader.class);
        sortedNumericDocValues = mock(SortedNumericDocValues.class);
        fetcher = new SortedNumericDocValuesFetcher(mappedFieldType, "test_field");
    }

    public void testFetchSingleValue() throws IOException {
        int docId = 1;
        long expectedValue = 123L;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(1);
        when(sortedNumericDocValues.nextValue()).thenReturn(expectedValue);

        List<Object> values = fetcher.fetch(leafReader, docId);

        assertEquals(1, values.size());
        assertEquals(expectedValue, values.getFirst());
    }

    public void testFetchMultipleValues() throws IOException {
        int docId = 1;
        long[] expectedValues = { 1L, 2L, 3L };
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(expectedValues.length);
        when(sortedNumericDocValues.nextValue()).thenReturn(expectedValues[0]).thenReturn(expectedValues[1]).thenReturn(expectedValues[2]);

        List<Object> values = fetcher.fetch(leafReader, docId);

        assertEquals(expectedValues.length, values.size());
        for (int i = 0; i < expectedValues.length; i++) {
            assertEquals(expectedValues[i], values.get(i));
        }
    }

    public void testFetchNoValues() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenReturn(sortedNumericDocValues);
        when(sortedNumericDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedNumericDocValues.docValueCount()).thenReturn(0);

        List<Object> values = fetcher.fetch(leafReader, docId);

        Assert.assertTrue(values.isEmpty());
    }

    public void testConvert() {
        Long value = 123L;
        String expectedDisplayValue = "123";
        when(mappedFieldType.valueForDisplay(value)).thenReturn(expectedDisplayValue);

        Object result = fetcher.convert(value);

        assertEquals(expectedDisplayValue, result);
        verify(mappedFieldType).valueForDisplay(value);
    }

    public void testWriteSingleValue() throws IOException {
        Long value = 123L;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(value)).thenReturn(value);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder, List.of(value));
        builder.endObject();

        String expected = "{\"test_field\":123}";
        assertEquals(expected, builder.toString());
    }

    public void testWriteMultipleValues() throws IOException {
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(anyLong())).thenReturn(1, 2, 3);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder, List.of(1L, 2L, 3L));
        builder.endObject();

        String expected = "{\"test_field\":[1,2,3]}";
        assertEquals(expected, builder.toString());
    }

    public void testFetchThrowsIOException() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedNumericDocValues("test_field")).thenThrow(new IOException("Test exception"));

        assertThrows(IOException.class, () -> fetcher.fetch(leafReader, docId));
    }
}
