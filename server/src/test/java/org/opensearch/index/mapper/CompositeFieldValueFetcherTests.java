/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CompositeFieldValueFetcherTests extends OpenSearchTestCase {

    private LeafReader mockReader;
    private static final String FIELD_NAME = "test_field";
    private static final int DOC_ID = 42;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockReader = mock(LeafReader.class);
    }

    public void testFetchReturnsFromFirstAvailableFetcher() throws IOException {
        // Setup: First fetcher returns empty, second returns values
        FieldValueFetcher fetcher1 = createMockFetcherWithValues(Collections.emptyList());
        FieldValueFetcher fetcher2 = createMockFetcherWithValues(List.of("value1", "value2"));
        FieldValueFetcher fetcher3 = createMockFetcherWithValues(List.of("value3")); // Should not be called

        List<FieldValueFetcher> fetchers = List.of(fetcher1, fetcher2, fetcher3);
        CompositeFieldValueFetcher composite = new CompositeFieldValueFetcher(FIELD_NAME, fetchers);

        // Execute
        List<Object> result = composite.fetch(mockReader, DOC_ID);

        // Verify
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals("CONVERTED:value1", result.get(0));
        assertEquals("CONVERTED:value2", result.get(1));

        // Verify fetcher3 was never called
        verify(fetcher1, times(1)).fetch(mockReader, DOC_ID);
        verify(fetcher2, times(1)).fetch(mockReader, DOC_ID);
        verify(fetcher3, never()).fetch(any(), anyInt());
    }

    public void testFetchReturnsFromFirstFetcherWhenAvailable() throws IOException {
        // Setup: First fetcher has values
        FieldValueFetcher fetcher1 = createMockFetcherWithValues(List.of("first"));
        FieldValueFetcher fetcher2 = createMockFetcherWithValues(List.of("second"));

        List<FieldValueFetcher> fetchers = List.of(fetcher1, fetcher2);
        CompositeFieldValueFetcher composite = new CompositeFieldValueFetcher(FIELD_NAME, fetchers);

        // Execute
        List<Object> result = composite.fetch(mockReader, DOC_ID);

        // Verify
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals("CONVERTED:first", result.get(0));

        // Second fetcher should not be called
        verify(fetcher2, never()).fetch(any(), anyInt());
    }

    public void testFetchReturnsNullWhenNoFetcherHasValues() throws IOException {
        // Setup: All fetchers return empty or null
        FieldValueFetcher fetcher1 = createMockFetcherWithValues(Collections.emptyList());
        FieldValueFetcher fetcher2 = createMockFetcherWithValues(null);
        FieldValueFetcher fetcher3 = createMockFetcherWithValues(Collections.emptyList());

        List<FieldValueFetcher> fetchers = List.of(fetcher1, fetcher2, fetcher3);
        CompositeFieldValueFetcher composite = new CompositeFieldValueFetcher(FIELD_NAME, fetchers);

        // Execute
        List<Object> result = composite.fetch(mockReader, DOC_ID);

        // Verify
        assertNull(result);

        // All fetchers should have been tried
        verify(fetcher1, times(1)).fetch(mockReader, DOC_ID);
        verify(fetcher2, times(1)).fetch(mockReader, DOC_ID);
        verify(fetcher3, times(1)).fetch(mockReader, DOC_ID);
    }

    public void testFetchWithEmptyFetchersList() throws IOException {
        CompositeFieldValueFetcher composite = new CompositeFieldValueFetcher(FIELD_NAME, Collections.emptyList());

        List<Object> result = composite.fetch(mockReader, DOC_ID);

        assertNull(result);
    }

    public void testFetchPropagatesIOException() throws IOException {
        FieldValueFetcher fetcher = mock(FieldValueFetcher.class);
        IOException expectedException = new IOException("Test exception");
        when(fetcher.fetch(mockReader, DOC_ID)).thenThrow(expectedException);

        CompositeFieldValueFetcher composite = new CompositeFieldValueFetcher(FIELD_NAME, List.of(fetcher));

        IOException thrown = expectThrows(IOException.class, () -> composite.fetch(mockReader, DOC_ID));

        assertEquals("Test exception", thrown.getMessage());
    }

    private FieldValueFetcher createMockFetcherWithValues(List<Object> values) throws IOException {
        FieldValueFetcher fetcher = mock(FieldValueFetcher.class);
        when(fetcher.fetch(mockReader, DOC_ID)).thenReturn(values);
        when(fetcher.convert(any())).thenAnswer(invocation -> {
            Object arg = invocation.getArgument(0);
            return arg == null ? null : "CONVERTED:" + arg;
        });
        return fetcher;
    }
}
