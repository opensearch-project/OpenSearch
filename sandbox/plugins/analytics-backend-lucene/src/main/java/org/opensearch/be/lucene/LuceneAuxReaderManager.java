/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reader manager for a co-located <em>auxiliary</em> Lucene index (Engine-4's element index, format
 * {@code aux__lucene__nested}). Unlike {@link LuceneReaderManager}, which reads the shard's shared
 * committed index via {@link org.opensearch.index.engine.exec.commit.IndexStoreProvider}, the element
 * index lives in its own per-generation {@code lucene_gen_<auxGeneration>} directories and is committed
 * on disk with its own {@code segments_N}. So this manager opens a {@link DirectoryReader} directly on
 * the aux segment's {@link WriterFileSet#directory()} from each {@link CatalogSnapshot}.
 *
 * <p>v1 handles a single aux segment per snapshot (the single-flush case, and the post-merge case where
 * {@code ElementIndexMerger} has consolidated to one). Multiple un-merged aux generations in one
 * snapshot would need the readers unioned (a {@code MultiReader}); that is a follow-up — see
 * {@code MustangDevConfig design/nested-field-support/13}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneAuxReaderManager implements EngineReaderManager<LuceneReader> {

    private static final Logger logger = LogManager.getLogger(LuceneAuxReaderManager.class);

    private final DataFormat auxFormat;
    private final Map<Long, LuceneReader> readers = new ConcurrentHashMap<>();

    public LuceneAuxReaderManager(DataFormat auxFormat) {
        this.auxFormat = auxFormat;
    }

    @Override
    public LuceneReader getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        // Null (not an exception) when this snapshot has no element segment yet — e.g. an empty index,
        // or before the first nested document flushes. acquireReader tolerates a null reader (it just
        // isn't added to the per-format reader map), so a non-nested query and recovery both proceed;
        // a nested query only fetches this reader when the mapping declares a nested field AND data has
        // flushed, by which point afterRefresh has registered it.
        return readers.get(catalogSnapshot.getId());
    }

    @Override
    public void beforeRefresh() throws IOException {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) throws IOException {
        if (readers.containsKey(catalogSnapshot.getId())) {
            return;
        }
        // The element segment(s) for this snapshot, by their auxiliary generation.
        DirectoryReader directoryReader = null;
        Map<Long, String> generationToSegmentName = new HashMap<>();
        for (Segment seg : catalogSnapshot.getSegments()) {
            WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(auxFormat.name());
            if (wfs == null) {
                continue;
            }
            if (directoryReader != null) {
                // v1 limitation: more than one un-merged element segment in a snapshot.
                logger.warn(
                    "Element index has more than one un-merged segment in snapshot [{}]; v1 reads only the first. "
                        + "Force-merge to consolidate (see design 13).",
                    catalogSnapshot.getId()
                );
                break;
            }
            Directory dir = new MMapDirectory(Path.of(wfs.directory()));
            directoryReader = DirectoryReader.open(dir);
            // Match this snapshot's aux generation to the on-disk segment name (single leaf per aux dir).
            for (var ctx : directoryReader.leaves()) {
                SegmentReader sr = (SegmentReader) ctx.reader();
                generationToSegmentName.put(seg.generation(), sr.getSegmentInfo().info.name);
            }
        }
        if (directoryReader == null) {
            // No element segment yet (e.g. a snapshot before any nested doc flushed). Register nothing;
            // getReader will surface a clear error if a nested filter is somehow attempted against it.
            return;
        }
        readers.put(catalogSnapshot.getId(), new LuceneReader(directoryReader, generationToSegmentName));
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) throws IOException {
        LuceneReader reader = readers.remove(catalogSnapshot.getId());
        if (reader != null) {
            reader.directoryReader().close();
        }
    }

    @Override
    public void onFilesDeleted(Collection<String> files) throws IOException {}

    @Override
    public void onFilesAdded(Collection<String> files) throws IOException {}

    @Override
    public void close() throws IOException {
        for (LuceneReader reader : readers.values()) {
            reader.directoryReader().close();
        }
        readers.clear();
    }
}
