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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.be.lucene.index.LuceneReplicaCommitter;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.SuppressForbidden;
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
@SuppressForbidden(reason = "reference counting is required here")
public class LuceneReaderManager implements EngineReaderManager<LuceneReader> {

    private final DataFormat dataFormat;
    private final Map<Long, LuceneReader> readers;
    private volatile DirectoryReader initialReader;
    private volatile DirectoryReader currentReader;
    private final CheckedBiFunction<DirectoryReader, SegmentInfos, DirectoryReader, IOException> readerRefresher;

    /**
     * Creates a new LuceneReaderManager.
     *
     * @param dataFormat      the data format this reader manager serves
     * @param initialReader   the initial DirectoryReader, must not be null
     * @param readers         shared map of generation to DirectoryReader for segment-level reader reuse
     * @param readerRefresher function that opens a refreshed reader given the current reader and new
     *                        {@link SegmentInfos}; returns {@code null} if no refresh is needed
     * @throws NullPointerException if initialReader is null
     */
    public LuceneReaderManager(
        DataFormat dataFormat,
        DirectoryReader initialReader,
        Map<Long, LuceneReader> readers,
        CheckedBiFunction<DirectoryReader, SegmentInfos, DirectoryReader, IOException> readerRefresher
    ) {
        this.dataFormat = dataFormat;
        Objects.requireNonNull(initialReader, "initialReader must not be null");
        this.initialReader = initialReader;
        this.currentReader = initialReader;
        this.readers = readers;
        this.readerRefresher = readerRefresher;
    }

    @Override
    public LuceneReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        LuceneReader reader = readers.get(catalogSnapshot.getId());
        if (reader == null) {
            throw new IllegalStateException("No reader available for catalog snapshot [version=" + catalogSnapshot.getId() + "]");
        }
        return reader;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // no-op
    }

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (didRefresh == false || readers.containsKey(catalogSnapshot.getId())) {
            return;
        }
        DirectoryReader refreshed = readerRefresher.apply(currentReader, LuceneReplicaCommitter.getSegmentInfos(catalogSnapshot));
        if (refreshed != null) {
            currentReader = refreshed;
        } else {
            currentReader.incRef();
        }
        // Catches refresh/merge-apply races where the refreshed reader's leaves disagree
        // with the catalog snapshot being registered.
        assert readersAreSame(catalogSnapshot, currentReader);

        Map<Long, String> generationToSegmentName = buildGenerationToSegmentName(catalogSnapshot, currentReader.leaves());
        readers.put(catalogSnapshot.getId(), new LuceneReader(currentReader, generationToSegmentName));
    }

    private static Map<Long, String> buildGenerationToSegmentName(CatalogSnapshot catalogSnapshot, List<LeafReaderContext> leaves) {
        // Index leaves by their file set → segment name
        Map<Set<String>, String> filesToSegName = new HashMap<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            SegmentReader sr = (SegmentReader) leaves.get(i).reader();
            try {
                filesToSegName.put(new HashSet<>(sr.getSegmentInfo().files()), sr.getSegmentInfo().info.name);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to read files for leaf " + i, e);
            }
        }

        // Match catalog segments to leaves via file sets
        Map<Long, String> out = new HashMap<>();
        for (Segment seg : catalogSnapshot.getSegments()) {
            WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
            if (wfs == null) {
                continue;
            }
            String segName = filesToSegName.get(wfs.files());
            if (segName == null) {
                throw new IllegalStateException(
                    "Catalog segment gen=" + seg.generation() + " files=" + wfs.files() + " has no matching Lucene leaf"
                );
            }
            out.put(seg.generation(), segName);
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
     * @param reader         DirectoryReader whose leaves' generations are the actual set
     * @return {@code true} iff both lists contain the same generations in the same (sorted) order
     */
    private boolean readersAreSame(CatalogSnapshot catalogSnapshot, DirectoryReader reader) {
        Collection<Long> generationsReferenced = catalogSnapshot.getSegments().stream().map(Segment::generation).sorted().toList();
        return generationsReferenced.equals(collectReferencedGenerations(reader));
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
        LuceneReader reader = readers.remove(catalogSnapshot.getId());
        if (reader != null) {
            reader.directoryReader().decRef();
        }
        releaseInitialReader();
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
            reader.directoryReader().decRef();
        }
        readers.clear();
        releaseInitialReader();
    }

    /**
     * Releases the initial {@link DirectoryReader} opened by
     * {@link LuceneSearchBackEnd#createReaderManager} and nulls out the field. The check
     * makes the operation idempotent so it is safe to invoke from both {@link #close()}
     * and {@link #onDeleted(CatalogSnapshot)} without double-freeing.
     * <p>
     * The initial reader is owned by the manager (not the {@link #readers} map), so it is
     * not covered by the per-snapshot decRef loop in {@code close()}. Without this release,
     * the {@link java.nio.channels.FileChannel}s held by its {@link SegmentReader}s would
     * leak until JVM exit (detected by Lucene's {@code LeakFS} at suite teardown).
     */
    private void releaseInitialReader() throws IOException {
        if (initialReader != null) {
            initialReader.decRef();
            initialReader = null;
        }
    }
}
