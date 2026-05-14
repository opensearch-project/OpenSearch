/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * Lucene implementation of {@link EngineReaderManager}.
 * <p>
 * Constructed with a {@link DataFormat} and an initial {@link DirectoryReader}
 * (typically opened from an IndexWriter). Maintains a map of {@link CatalogSnapshot}
 * to {@link LuceneReader} so each snapshot gets the reader that was current
 * at the time of its refresh. On each {@link #afterRefresh}, the current reader is
 * refreshed via {@link DirectoryReader#openIfChanged} and paired with a
 * {@code writer_generation → leaf index} map built by matching the catalog's
 * {@link WriterFileSet#files()} against each leaf's {@code SegmentCommitInfo.files()}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements EngineReaderManager<LuceneReader> {

    private final DataFormat dataFormat;
    private final Map<CatalogSnapshot, LuceneReader> readers = new HashMap<>();
    private volatile DirectoryReader currentReader;

    /**
     * Creates a new LuceneReaderManager.
     *
     * @param dataFormat the data format this reader manager serves
     * @param initialReader the initial DirectoryReader, must not be null
     * @throws NullPointerException if initialReader is null
     */
    public LuceneReaderManager(DataFormat dataFormat, DirectoryReader initialReader) {
        this.dataFormat = dataFormat;
        Objects.requireNonNull(initialReader, "initialReader must not be null");
        this.currentReader = initialReader;
    }

    @Override
    public LuceneReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        LuceneReader reader = readers.get(catalogSnapshot);
        if (reader == null) {
            throw new IllegalStateException("No reader available for catalog snapshot [gen=" + catalogSnapshot.getGeneration() + "]");
        }
        return reader;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no-op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false || readers.containsKey(catalogSnapshot)) {
            return;
        }
        DirectoryReader refreshed = DirectoryReader.openIfChanged(currentReader);
        if (refreshed != null) {
            assert readersAreSame(catalogSnapshot, refreshed);
            currentReader = refreshed;
        }

        Map<Long, Integer> generationToLeaf = buildGenerationToLeaf(catalogSnapshot, currentReader.leaves());
        readers.put(catalogSnapshot, new LuceneReader(currentReader, generationToLeaf));
    }

    private static Map<Long, Integer> buildGenerationToLeaf(CatalogSnapshot catalogSnapshot, List<LeafReaderContext> leaves) {
        // Index leaves by their file set
        Map<Set<String>, Integer> filesToLeaf = new HashMap<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            SegmentReader sr = (SegmentReader) leaves.get(i).reader();
            try {
                filesToLeaf.put(new HashSet<>(sr.getSegmentInfo().files()), i);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read files for leaf " + i, e);
            }
        }

        // Match catalog segments to leaves via file sets
        Map<Long, Integer> out = new HashMap<>();
        for (Segment seg : catalogSnapshot.getSegments()) {
            WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
            if (wfs == null) {
                continue;
            }
            Integer leafIdx = filesToLeaf.get(wfs.files());
            if (leafIdx == null) {
                throw new IllegalStateException(
                    "Catalog segment gen=" + seg.generation() + " files=" + wfs.files() + " has no matching Lucene leaf"
                );
            }
            out.put(seg.generation(), leafIdx);
        }
        return out;
    }

    /**
     * Consistency check: verifies that the refreshed {@link DirectoryReader} reflects exactly
     * the set of segments the given {@link CatalogSnapshot} references. Compares the sorted
     * list of writer generations drawn from the snapshot's {@link Segment Segments} against
     * the sorted list of writer generations read off each leaf of the reader (via the
     * {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE} stamped
     * onto every Lucene segment at write time).
     *
     * <p>Used only in an {@code assert} to catch refresh/catalog drift in test builds — if
     * this ever returns {@code false} in production, it means a Lucene reader has been paired
     * with the wrong catalog snapshot.
     *
     * @param catalogSnapshot catalog snapshot whose referenced generations are the expected set
     * @param readers         DirectoryReader whose leaves' generations are the actual set
     * @return {@code true} iff both lists contain the same generations in the same (sorted) order
     */
    private boolean readersAreSame(CatalogSnapshot catalogSnapshot, DirectoryReader readers) {
        Collection<Long> generationsReferenced = catalogSnapshot.getSegments().stream().map(Segment::generation).sorted().toList();
        return generationsReferenced.equals(collectReferencedGenerations(readers));
    }

    /**
     * Extracts the writer generation from each leaf of the given {@link DirectoryReader} and
     * returns them as a sorted list. Each leaf's {@link SegmentReader} carries a
     * {@link SegmentCommitInfo} whose {@code SegmentInfo} is stamped with the
     * {@link org.opensearch.be.lucene.index.LuceneWriter#WRITER_GENERATION_ATTRIBUTE} when the
     * segment is written; parsing that attribute yields the generation that produced the leaf.
     *
     * @param reader the DirectoryReader to inspect
     * @return generations of all leaves, sorted ascending
     * @throws NumberFormatException if a leaf is missing the writer-generation attribute or
     *                               its value is not parseable as a long (indicates a segment
     *                               not produced by {@link org.opensearch.be.lucene.index.LuceneWriter})
     * @throws ClassCastException    if any leaf reader is not a {@link SegmentReader}
     */
    private Collection<Long> collectReferencedGenerations(DirectoryReader reader) {
        return reader.leaves().stream().map(lrc -> {
            SegmentReader segmentReader = (SegmentReader) lrc.reader();
            SegmentCommitInfo sci = segmentReader.getSegmentInfo();
            return Long.parseLong(sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE));
        }).sorted().toList();
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        LuceneReader reader = readers.remove(catalogSnapshot);
        if (reader != null) {
            reader.directoryReader().close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {
        // no-op
    }

    @Override
    public void close() throws IOException {
        for (LuceneReader reader : readers.values()) {
            reader.directoryReader().close();
        }
        readers.clear();
    }
}
