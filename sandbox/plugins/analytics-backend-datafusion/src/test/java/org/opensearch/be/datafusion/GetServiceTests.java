/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.spi.DocumentLookupService;
import org.opensearch.analytics.spi.DocumentRowReader;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.DocumentMetadataResolver.DocumentMetadata;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GetServiceTests extends OpenSearchTestCase {

    private static final Index INDEX = new Index("idx", "uuid");

    private DocumentRowReader mockExecutor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockExecutor = mock(DocumentRowReader.class);
    }

    public void testGetById_nullResolver_returnsNotFound() throws IOException {
        DocumentMetadataResolver resolver = mock(DocumentMetadataResolver.class);
        when(resolver.resolveMetadata(any(), eq("doc1"))).thenReturn(null);

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        DocumentLookupService service = new GetService(mockExecutor).documentLookupService(resolver);
        DocumentLookupResult result = service.getById("doc1", mockReader, INDEX);
        assertFalse(result.exists());
        assertEquals("doc1", result.id());
    }

    public void testGetById_docFound_returnsFromParquet() throws IOException {
        DocumentMetadataResolver resolver = mock(DocumentMetadataResolver.class);
        DocumentMetadata metadata = new DocumentMetadata("doc1", 0L, 1L);
        when(resolver.resolveMetadata(any(), eq("doc1"))).thenReturn(metadata);

        DocumentRowReader executor = mock(DocumentRowReader.class);
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        WriterFileSet pset = new WriterFileSet("/dir", 1L, Set.of("f.parquet"), 1L, 100L);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);
        when(snapshot.findFileSet("parquet", 1L)).thenReturn(pset);
        when(executor.formatName()).thenReturn("parquet");
        when(executor.executeSingleRow(0L, pset)).thenReturn(
            Map.of("_id", "doc1", "_seq_no", 5L, "_primary_term", 1L, "_version", 2L, "field1", "value1")
        );

        DocumentLookupService service = new GetService(executor).documentLookupService(resolver);
        DocumentLookupResult result = service.getById("doc1", mockReader, INDEX);

        assertTrue("doc should be found", result.exists());
        assertEquals("doc1", result.id());
        assertEquals(5L, result.seqNo());
        assertEquals(2L, result.version());
    }

    public void testGetById_executorReturnsNull_throwsISE() throws IOException {
        DocumentMetadataResolver resolver = mock(DocumentMetadataResolver.class);
        DocumentMetadata metadata = new DocumentMetadata("doc1", 0L, 1L);
        when(resolver.resolveMetadata(any(), eq("doc1"))).thenReturn(metadata);

        DocumentRowReader executor = mock(DocumentRowReader.class);
        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        WriterFileSet pset = new WriterFileSet("/dir", 1L, Set.of("f.parquet"), 1L, 100L);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);
        when(snapshot.findFileSet("parquet", 1L)).thenReturn(pset);
        when(executor.formatName()).thenReturn("parquet");
        when(executor.executeSingleRow(0L, pset)).thenReturn(null);

        DocumentLookupService service = new GetService(executor).documentLookupService(resolver);
        expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, INDEX));
    }

    public void testGetById_fileSetNull_throwsISE() throws IOException {
        DocumentMetadataResolver resolver = mock(DocumentMetadataResolver.class);
        DocumentMetadata metadata = new DocumentMetadata("doc1", 0L, 1L);
        when(resolver.resolveMetadata(any(), eq("doc1"))).thenReturn(metadata);

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(mockReader.catalogSnapshot()).thenReturn(snapshot);
        when(snapshot.findFileSet("parquet", 1L)).thenReturn(null);
        when(mockExecutor.formatName()).thenReturn("parquet");

        DocumentLookupService service = new GetService(mockExecutor).documentLookupService(resolver);
        expectThrows(IllegalStateException.class, () -> service.getById("doc1", mockReader, INDEX));
    }

    public void testGetById_resolverThrows_propagates() throws IOException {
        DocumentMetadataResolver resolver = mock(DocumentMetadataResolver.class);
        when(resolver.resolveMetadata(any(), eq("doc1"))).thenThrow(new IOException("reader closed"));

        IndexReaderProvider.Reader mockReader = mock(IndexReaderProvider.Reader.class);
        DocumentLookupService service = new GetService(mockExecutor).documentLookupService(resolver);
        expectThrows(IOException.class, () -> service.getById("doc1", mockReader, INDEX));
    }

    public void testExtractLong_number() throws Exception {
        assertEquals(42L, invokeExtractLong(Map.of("k", 42), "k", -1L));
        assertEquals(42L, invokeExtractLong(Map.of("k", 42L), "k", -1L));
        assertEquals(42L, invokeExtractLong(Map.of("k", 42.9), "k", -1L));
    }

    public void testExtractLong_string() throws Exception {
        assertEquals(99L, invokeExtractLong(Map.of("k", "99"), "k", -1L));
    }

    public void testExtractLong_missing_returnsFallback() throws Exception {
        assertEquals(-1L, invokeExtractLong(Map.of(), "k", -1L));
    }

    public void testExtractLong_invalidString_returnsFallback() throws Exception {
        assertEquals(-1L, invokeExtractLong(Map.of("k", "abc"), "k", -1L));
    }

    public void testNoopResolver_returnsNull() throws IOException {
        assertNull(DocumentMetadataResolver.NOOP.resolveMetadata(mock(IndexReaderProvider.Reader.class), "any"));
    }

    private static long invokeExtractLong(Map<String, Object> row, String key, long fallback) {
        return DocumentLookupService.extractLong(row, key, fallback);
    }
}
