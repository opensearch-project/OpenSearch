/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SortedSetDocValuesFetcherTests extends OpenSearchTestCase {
    private MappedFieldType mappedFieldType;
    private LeafReader leafReader;
    private SortedSetDocValues sortedSetDocValues;

    private SortedSetDocValuesFetcher fetcher;

    @Before
    public void setupTest() {
        mappedFieldType = mock(MappedFieldType.class);
        leafReader = mock(LeafReader.class);
        sortedSetDocValues = mock(SortedSetDocValues.class);
        fetcher = new SortedSetDocValuesFetcher(mappedFieldType, "test_field");
    }

    public void testFetchSingleValue() throws IOException {
        int docId = 1;
        String value = "123";
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedSetDocValues("test_field")).thenReturn(sortedSetDocValues);
        when(sortedSetDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedSetDocValues.docValueCount()).thenReturn(1);
        when(sortedSetDocValues.lookupOrd(anyLong())).thenReturn(new BytesRef(value.getBytes(StandardCharsets.UTF_8)));

        List<Object> res = fetcher.fetch(leafReader, docId);

        assertEquals(1, res.size());
        assertEquals(value, DocValueFormat.RAW.format((BytesRef) res.getFirst()));
    }

    public void testFetchMultipleValues() throws IOException {
        int docId = 1;
        String[] values = { "1", "2", "3" };
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedSetDocValues("test_field")).thenReturn(sortedSetDocValues);
        when(sortedSetDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedSetDocValues.docValueCount()).thenReturn(values.length);
        when(sortedSetDocValues.lookupOrd(anyLong())).thenReturn(new BytesRef(values[0].getBytes(StandardCharsets.UTF_8)))
            .thenReturn(new BytesRef(values[1].getBytes(StandardCharsets.UTF_8)))
            .thenReturn(new BytesRef(values[2].getBytes(StandardCharsets.UTF_8)));

        List<Object> res = fetcher.fetch(leafReader, docId);

        assertEquals(values.length, res.size());
        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], DocValueFormat.RAW.format((BytesRef) res.get(i)));
        }
    }

    public void testFetchNoValues() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedSetDocValues("test_field")).thenReturn(sortedSetDocValues);
        when(sortedSetDocValues.advanceExact(docId)).thenReturn(true);
        when(sortedSetDocValues.docValueCount()).thenReturn(0);

        List<Object> res = fetcher.fetch(leafReader, docId);
        Assert.assertTrue(res.isEmpty());
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
        List<Object> values = new ArrayList<>();
        values.add(value);
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(value)).thenReturn(value);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder, values);
        builder.endObject();

        String expected = "{\"test_field\":123}";
        assertEquals(expected, builder.toString());
    }

    public void testWriteMultipleValues() throws IOException {
        List<Object> values = List.of(1L, 2L, 3L);
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(anyLong())).thenReturn(1, 2, 3);
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();

        fetcher.write(builder, values);
        builder.endObject();

        String expected = "{\"test_field\":[1,2,3]}";
        assertEquals(expected, builder.toString());
    }

    public void testFetchThrowsIOException() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(leafReader.getSortedSetDocValues("test_field")).thenThrow(new IOException("Test exception"));

        assertThrows(IOException.class, () -> fetcher.fetch(leafReader, docId));
    }
}
