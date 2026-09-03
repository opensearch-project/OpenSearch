/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.uid.VersionsAndSeqNoResolver;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;

import java.io.IOException;

/**
 * Lucene-backed {@link DocumentMetadataResolver}: an {@code _id} lookup yields the row location
 * ({@code rowId} from {@code __row_id__} doc values, {@code writerGeneration} from the segment
 * attribute). Version fields are not read here. An empty reader or absent id reports "not found".
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class LuceneDocumentResolver implements DocumentMetadataResolver {

    @Override
    public DocumentMetadata resolveMetadata(IndexReaderProvider.Reader reader, String id) throws IOException {
        DirectoryReader luceneReader = directoryReader(reader);
        if (luceneReader.numDocs() == 0) {
            return null;
        }
        LeafDoc located = locate(luceneReader, id);
        if (located == null) {
            return null;
        }
        LeafReader leaf = located.leaf().reader();
        int localDocId = located.localDocId();
        return new DocumentMetadata(id, readRowId(leaf, localDocId), readWriterGeneration(leaf));
    }

    private long readRowId(LeafReader leaf, int localDocId) throws IOException {
        return value(leaf.getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD), localDocId);
    }

    private long readWriterGeneration(LeafReader leaf) {
        LeafReader unwrapped = FilterLeafReader.unwrap(leaf);
        if (!(unwrapped instanceof SegmentReader segmentReader)) {
            throw new IllegalStateException("Expected SegmentReader leaf, got " + unwrapped.getClass());
        }
        String genAttr = segmentReader.getSegmentInfo().info.getAttribute("writer_generation");
        if (genAttr == null) {
            throw new IllegalStateException("Leaf segment missing writer_generation attribute");
        }
        try {
            return Long.parseLong(genAttr);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid writer_generation attribute: [" + genAttr + "]", e);
        }
    }

    private long value(SortedNumericDocValues dv, int docId) throws IOException {
        if (dv == null) {
            throw new IllegalStateException("Leaf segment missing " + DocumentInput.ROW_ID_FIELD + " doc values");
        }
        if (!dv.advanceExact(docId)) {
            throw new IllegalStateException("docId " + docId + " has no " + DocumentInput.ROW_ID_FIELD + " value");
        }
        return dv.nextValue();
    }

    private DirectoryReader directoryReader(IndexReaderProvider.Reader reader) {
        LuceneReader luceneReader = reader.getReader(LucenePlugin.DATA_FORMAT, LuceneReader.class);
        return luceneReader.directoryReader();
    }

    private LeafDoc locate(DirectoryReader luceneReader, String id) throws IOException {
        Term term = new Term(IdFieldMapper.NAME, Uid.encodeId(id));
        // Reuse the core per-thread, per-segment _id lookup cache (newest-segment-first, liveDocs-aware).
        // We only need the doc location here; version/seqNo/primaryTerm come from the parquet row.
        VersionsAndSeqNoResolver.DocIdAndSeqNo hit = VersionsAndSeqNoResolver.loadDocId(luceneReader, term);
        return hit == null ? null : new LeafDoc(hit.context, hit.docId);
    }

    private record LeafDoc(LeafReaderContext leaf, int localDocId) {
    }
}
