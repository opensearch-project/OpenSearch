/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.fieldvisitor.SingleFieldsVisitor;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StoredFieldFetcherTests extends OpenSearchTestCase {
    private StoredFieldFetcher fetcher;
    private MappedFieldType mappedFieldType;
    private LeafReader leafReader;
    private StoredFields storedFields;

    @Before
    public void setupTest() {
        mappedFieldType = mock(MappedFieldType.class);
        leafReader = mock(LeafReader.class);
        storedFields = mock(StoredFields.class);
        fetcher = new StoredFieldFetcher(mappedFieldType, "test_field");
    }

    public void testFetchStoredField() throws IOException {
        int docId = 1;
        when(leafReader.storedFields()).thenReturn(storedFields);
        doNothing().when(storedFields).document(eq(docId), any(SingleFieldsVisitor.class));

        fetcher.fetch(leafReader, docId);

        verify(leafReader).storedFields();
        verify(storedFields).document(eq(docId), any(SingleFieldsVisitor.class));
    }

    public void testFetchThrowsIOException() throws IOException {
        int docId = 1;
        when(leafReader.storedFields()).thenReturn(storedFields);
        doThrow(new IOException("Test exception")).when(storedFields).document(eq(docId), any(SingleFieldsVisitor.class));

        assertThrows(IOException.class, () -> fetcher.fetch(leafReader, docId));
    }

    public void testMultipleFetchCalls() throws IOException {
        int docId1 = 1;
        int docId2 = 2;
        when(leafReader.storedFields()).thenReturn(storedFields);

        fetcher.fetch(leafReader, docId1);
        fetcher.fetch(leafReader, docId2);

        verify(leafReader, times(2)).storedFields();
        verify(storedFields).document(eq(docId1), any(SingleFieldsVisitor.class));
        verify(storedFields).document(eq(docId2), any(SingleFieldsVisitor.class));
    }

    public void testNullStoredFields() throws IOException {
        when(leafReader.storedFields()).thenReturn(null);

        assertThrows(NullPointerException.class, () -> fetcher.fetch(leafReader, 1));
    }

    public void testWriteSingleValue() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay("123")).thenReturn("123");
        when(leafReader.storedFields()).thenReturn(storedFields);
        FieldInfo mockFieldInfo = new FieldInfo(
            "test_field",
            1,
            false,
            false,
            true,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        doAnswer(invocation -> {
            SingleFieldsVisitor visitor = invocation.getArgument(1);
            visitor.stringField(mockFieldInfo, "123");
            return null;
        }).when(storedFields).document(eq(docId), any(StoredFieldVisitor.class));

        List<Object> values = fetcher.fetch(leafReader, docId);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        fetcher.write(builder, values);
        builder.endObject();

        String expected = "{\"test_field\":\"123\"}";
        assertEquals(expected, builder.toString());
    }

    public void testWriteMultiValue() throws IOException {
        int docId = 1;
        when(mappedFieldType.name()).thenReturn("test_field");
        when(mappedFieldType.valueForDisplay(anyString())).thenReturn("1", "2", "3");
        when(leafReader.storedFields()).thenReturn(storedFields);
        FieldInfo mockFieldInfo = new FieldInfo(
            "test_field",
            1,
            false,
            false,
            true,
            IndexOptions.NONE,
            DocValuesType.NONE,
            DocValuesSkipIndexType.NONE,
            -1,
            Collections.emptyMap(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
        doAnswer(invocation -> {
            SingleFieldsVisitor visitor = invocation.getArgument(1);
            visitor.stringField(mockFieldInfo, "1");
            visitor.stringField(mockFieldInfo, "2");
            visitor.stringField(mockFieldInfo, "3");
            return null;
        }).when(storedFields).document(eq(docId), any(StoredFieldVisitor.class));

        List<Object> values = fetcher.fetch(leafReader, docId);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        fetcher.write(builder, values);
        builder.endObject();

        String expected = "{\"test_field\":[\"1\",\"2\",\"3\"]}";
        assertEquals(expected, builder.toString());
    }
}
