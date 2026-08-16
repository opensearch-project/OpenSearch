/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.get.DocumentLookupResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DocumentLookupService}, the Core orchestrator for document lookup.
 *
 * <p>All collaborators are interfaces ({@link DocumentMetadataResolver}, {@link DocumentRowReader},
 * {@link IndexReaderProvider.Reader}) or an abstract class ({@link CatalogSnapshot}) and are mocked.
 * {@link Segment}/{@link WriterFileSet} are real objects constructed via their builders since they
 * are final records / sealed classes.
 *
 * <p>Coverage:
 * <ul>
 *   <li>{@code getById} — not-found, success (reserved/underscore field filtering), and the two
 *       Lucene/Parquet inconsistency ISE paths (missing file set, missing row).</li>
 *   <li>{@code getVersionMetadata} — not-found, version-field extraction with null source, and
 *       fallback defaults when version fields are absent.</li>
 *   <li>{@code getDocsAboveSeqNo} — rows without {@code _id} skipped, empty/wrong-format file sets
 *       excluded from the backend scan, and empty result handling.</li>
 *   <li>{@code extractLong} — Number, missing key, parsable String, and unparsable String.</li>
 * </ul>
 */
public class DocumentLookupServiceTests extends OpenSearchTestCase {

    private static final String FORMAT = "parquet";

    private static final Index INDEX = new Index("idx", "uuid");

    private DocumentMetadataResolver resolver;
    private DocumentRowReader executor;
    private IndexReaderProvider.Reader reader;
    private CatalogSnapshot snapshot;
    private DocumentLookupService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        resolver = mock(DocumentMetadataResolver.class);
        executor = mock(DocumentRowReader.class);
        reader = mock(IndexReaderProvider.Reader.class);
        snapshot = mock(CatalogSnapshot.class);
        when(reader.catalogSnapshot()).thenReturn(snapshot);
        when(executor.formatName()).thenReturn(FORMAT);
        service = new DocumentLookupService(resolver, executor);
    }

    // ---- helpers ------------------------------------------------------------

    private static WriterFileSet fileSet(long generation, String... files) {
        WriterFileSet.Builder b = WriterFileSet.builder()
            .directory(Path.of("/tmp/idx"))
            .writerGeneration(generation)
            .addNumRows(files.length);
        for (String f : files) {
            b.addFile(f);
        }
        return b.build();
    }

    private static DocumentMetadataResolver.DocumentMetadata metadata(String id, long rowId, long generation) {
        return new DocumentMetadataResolver.DocumentMetadata(id, rowId, generation);
    }

    private static Map<String, Object> row(Object... kv) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            row.put((String) kv[i], kv[i + 1]);
        }
        return row;
    }

    // ---- getById ------------------------------------------------------------

    public void testGetById_notFoundWhenResolverReturnsNull() throws Exception {
        when(resolver.resolveMetadata(reader, "missing")).thenReturn(null);

        DocumentLookupResult result = service.getById("missing", reader, INDEX);

        assertFalse(result.exists());
        assertEquals("missing", result.id());
        assertEquals(Versions.NOT_FOUND, result.version());
        assertNull(result.source());
    }

    public void testGetById_successFiltersMetadataAndInternalColumns() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        WriterFileSet fs = fileSet(7L, "0.parquet");
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(fs);
        when(executor.executeSingleRow(0L, fs)).thenReturn(
            row("_id", "doc1", "_seq_no", 42L, "_primary_term", 2L, "_version", 5L, "__row_id__", 99L, "name", "alice", "age", 30)
        );

        DocumentLookupResult result = service.getById("doc1", reader, INDEX);

        assertTrue(result.exists());
        assertEquals("doc1", result.id());
        assertEquals(5L, result.version());
        assertEquals(42L, result.seqNo());
        assertEquals(2L, result.primaryTerm());

        String source = result.source().utf8ToString();
        // user fields retained
        assertTrue("user fields present: " + source, source.contains("\"name\":\"alice\""));
        assertTrue("user fields present: " + source, source.contains("\"age\":30"));
        // metadata-mapper fields excluded via the predicate
        assertFalse("metadata field excluded: " + source, source.contains("_seq_no"));
        assertFalse("metadata field excluded: " + source, source.contains("_version"));
        assertFalse("metadata field excluded: " + source, source.contains("\"_id\""));
        // non-mapper columns excluded via the constant guards (the regression this protects against)
        assertFalse("_primary_term excluded via constant: " + source, source.contains("_primary_term"));
        assertFalse("__row_id__ excluded via constant: " + source, source.contains("__row_id__"));
    }

    public void testGetById_nonMetadataUnderscoreFieldRetained() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        WriterFileSet fs = fileSet(7L, "0.parquet");
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(fs);
        // "_custom" is not a registered metadata field, not _primary_term, not __row_id__ -> kept in _source.
        when(executor.executeSingleRow(0L, fs)).thenReturn(row("_id", "doc1", "_seq_no", 1L, "_custom", "keepme", "name", "bob"));

        DocumentLookupResult result = service.getById("doc1", reader, INDEX);

        String source = result.source().utf8ToString();
        assertTrue("non-metadata underscore field retained: " + source, source.contains("\"_custom\":\"keepme\""));
        assertTrue("user field retained: " + source, source.contains("\"name\":\"bob\""));
        assertFalse("metadata field excluded: " + source, source.contains("_seq_no"));
    }

    public void testGetById_throwsISEWhenFileSetNull() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(null);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> service.getById("doc1", reader, INDEX));
        assertTrue(e.getMessage(), e.getMessage().contains("no matching file set"));
        assertTrue(e.getMessage(), e.getMessage().contains("doc1"));
    }

    public void testGetById_throwsISEWhenRowNull() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        WriterFileSet fs = fileSet(7L, "0.parquet");
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(fs);
        when(executor.executeSingleRow(0L, fs)).thenReturn(null);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> service.getById("doc1", reader, INDEX));
        assertTrue(e.getMessage(), e.getMessage().contains("backend returned no row"));
        assertTrue(e.getMessage(), e.getMessage().contains("doc1"));
    }

    // ---- getVersionMetadata -------------------------------------------------

    public void testGetVersionMetadata_notFoundWhenResolverReturnsNull() throws Exception {
        when(resolver.resolveMetadata(reader, "missing")).thenReturn(null);

        DocumentLookupResult result = service.getVersionMetadata("missing", reader, INDEX);

        assertFalse(result.exists());
        assertEquals(Versions.NOT_FOUND, result.version());
        assertNull(result.source());
    }

    public void testGetVersionMetadata_extractsVersionFieldsWithNullSource() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        WriterFileSet fs = fileSet(7L, "0.parquet");
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(fs);
        when(executor.executeSingleRow(0L, fs)).thenReturn(row("_seq_no", 42L, "_primary_term", 2L, "_version", 5L, "name", "alice"));

        DocumentLookupResult result = service.getVersionMetadata("doc1", reader, INDEX);

        assertTrue(result.exists());
        assertEquals(5L, result.version());
        assertEquals(42L, result.seqNo());
        assertEquals(2L, result.primaryTerm());
        // version-only hot path: source is never reconstructed
        assertNull(result.source());
        assertTrue(result.documentFields().isEmpty());
    }

    public void testGetVersionMetadata_defaultsWhenFieldsMissing() throws Exception {
        when(resolver.resolveMetadata(reader, "doc1")).thenReturn(metadata("doc1", 0L, 7L));
        WriterFileSet fs = fileSet(7L, "0.parquet");
        when(snapshot.findFileSet(FORMAT, 7L)).thenReturn(fs);
        // row exists (found) but carries no version fields
        when(executor.executeSingleRow(0L, fs)).thenReturn(row("name", "alice"));

        DocumentLookupResult result = service.getVersionMetadata("doc1", reader, INDEX);

        assertTrue(result.exists());
        assertEquals(Versions.NOT_FOUND, result.version());
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, result.seqNo());
        assertEquals(SequenceNumbers.UNASSIGNED_PRIMARY_TERM, result.primaryTerm());
    }

    // ---- getDocsAboveSeqNo --------------------------------------------------

    public void testGetDocsAboveSeqNo_buildsResultsOnlyForRowsWithId() throws Exception {
        Segment segment = Segment.builder(1L).addSearchableFiles(FORMAT, fileSet(1L, "0.parquet")).build();
        when(snapshot.getSegments()).thenReturn(List.of(segment));

        Map<String, Object> withId = row("_id", "d1", "_seq_no", 10L, "name", "alice");
        Map<String, Object> withoutId = row("_seq_no", 11L, "name", "bob");
        when(executor.executeRowsAboveSeqNo(anyList(), eq(5L))).thenReturn(List.of(withId, withoutId));

        List<DocumentLookupResult> results = service.getDocsAboveSeqNo(5L, reader, INDEX);

        assertEquals(1, results.size());
        assertEquals("d1", results.get(0).id());
        assertEquals(10L, results.get(0).seqNo());
        assertTrue(results.get(0).source().utf8ToString().contains("\"name\":\"alice\""));
    }

    public void testGetDocsAboveSeqNo_skipsEmptyAndWrongFormatFileSets() throws Exception {
        WriterFileSet good = fileSet(1L, "0.parquet");
        WriterFileSet empty = fileSet(2L); // no files -> excluded
        WriterFileSet lucene = fileSet(3L, "seg.cfs"); // different format -> excluded
        Segment s1 = Segment.builder(1L).addSearchableFiles(FORMAT, good).build();
        Segment s2 = Segment.builder(2L).addSearchableFiles(FORMAT, empty).build();
        Segment s3 = Segment.builder(3L).addSearchableFiles("lucene", lucene).build();
        when(snapshot.getSegments()).thenReturn(List.of(s1, s2, s3));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<WriterFileSet>> captor = ArgumentCaptor.forClass(List.class);
        when(executor.executeRowsAboveSeqNo(captor.capture(), eq(0L))).thenReturn(List.of());

        service.getDocsAboveSeqNo(0L, reader, INDEX);

        List<WriterFileSet> passed = captor.getValue();
        assertEquals(1, passed.size());
        assertEquals(good, passed.get(0));
    }

    public void testGetDocsAboveSeqNo_emptyWhenNoRows() throws Exception {
        when(snapshot.getSegments()).thenReturn(List.of());
        when(executor.executeRowsAboveSeqNo(anyList(), anyLong())).thenReturn(List.of());

        assertTrue(service.getDocsAboveSeqNo(5L, reader, INDEX).isEmpty());
    }

    // ---- extractLong --------------------------------------------------------

    public void testExtractLong_number() {
        assertEquals(5L, DocumentLookupService.extractLong(Map.of("k", 5L), "k", 99L));
        assertEquals(7L, DocumentLookupService.extractLong(Map.of("k", 7), "k", 99L));
    }

    public void testExtractLong_nullReturnsFallback() {
        assertEquals(99L, DocumentLookupService.extractLong(Map.of(), "k", 99L));
    }

    public void testExtractLong_parsesStringNumber() {
        assertEquals(123L, DocumentLookupService.extractLong(Map.of("k", "123"), "k", 99L));
    }

    public void testExtractLong_unparseableStringReturnsFallback() {
        assertEquals(99L, DocumentLookupService.extractLong(Map.of("k", "abc"), "k", 99L));
    }
}
