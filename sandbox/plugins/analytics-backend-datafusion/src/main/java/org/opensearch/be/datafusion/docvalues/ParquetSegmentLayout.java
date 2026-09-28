/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.core.index.shard.ShardId;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Resolves the Parquet file that backs a Lucene segment's Parquet-resident doc values, and the store
 * its bytes must be read through.
 *
 * <p>The backing file is no longer stamped onto the segment. The server used to write the resolved path
 * (and, on a warm shard, the native store pointer) onto {@code SegmentInfo} attributes; that stamping
 * has been removed and the plugin now owns the mapping. Segments reach this codec carrying only their
 * {@link #WRITER_GENERATION_ATTRIBUTE}, which is the correlation key: {@link #resolve} looks the segment's
 * generation up in the per-shard {@link ParquetSegmentBindings} that {@code DatafusionReaderManager}
 * populates from each refreshed catalog snapshot.
 *
 * <p>A shard tiered to warm keeps no local copy of its Parquet files, so the binding also carries the
 * native object store the bytes must be read through. The file's identity is the same either way: the
 * shard's native file registry is keyed by that same absolute path.
 */
public final class ParquetSegmentLayout {

    private static final Logger logger = LogManager.getLogger(ParquetSegmentLayout.class);

    /**
     * {@code SegmentInfo} attribute key holding the writer generation stamped onto the segment, as a
     * decimal string. The literal matches {@code LuceneWriter.WRITER_GENERATION_ATTRIBUTE} in the
     * analytics-backend-lucene plugin, which is plugin-private; the writer stamps the same generation
     * into the Parquet footer, so the two agree for the file written alongside a segment. This is the
     * correlation key {@link #resolve} uses to look a segment's Parquet binding up.
     */
    public static final String WRITER_GENERATION_ATTRIBUTE = "writer_generation";

    private ParquetSegmentLayout() {}

    /**
     * Where a segment's Parquet doc values are read from: the file's identity, plus the native store to
     * read it through ({@link ParquetColumnReader#LOCAL_STORE} for a file on local disk).
     */
    public record ParquetSource(Path file, long storePointer) {

        /** Whether the bytes come from a native object store rather than the local filesystem. */
        public boolean isRemote() {
            return storePointer != ParquetColumnReader.LOCAL_STORE;
        }
    }

    /**
     * Returns where {@code state}'s segment reads its Parquet doc values from, or {@code null} if the
     * segment carries no parseable {@link #WRITER_GENERATION_ATTRIBUTE}, no binding is registered for that
     * generation on {@code shardId}, or the binding names a local path that no longer exists.
     *
     * <p>The generation is looked up in {@code bindings}, which {@code DatafusionReaderManager} populated
     * from the catalog snapshot on refresh. The existence check applies only to a local file. A remote
     * file is not probed: it is not expected on this node's disk at all, and the store reports a genuinely
     * missing object on the first read.
     */
    public static ParquetSource resolve(SegmentReadState state, ShardId shardId, ParquetSegmentBindings bindings) {
        String genAttr = state.segmentInfo.getAttribute(WRITER_GENERATION_ATTRIBUTE);
        if (genAttr == null || genAttr.isEmpty()) {
            logger.debug(
                "segment {} carries no {} attribute; serving no Parquet doc values",
                state.segmentInfo.name,
                WRITER_GENERATION_ATTRIBUTE
            );
            return null;
        }
        final long generation;
        try {
            generation = Long.parseLong(genAttr);
        } catch (NumberFormatException e) {
            logger.debug("segment {} has unparseable {} attribute '{}'", state.segmentInfo.name, WRITER_GENERATION_ATTRIBUTE, genAttr);
            return null;
        }
        ParquetSegmentBindings.Binding binding = bindings.resolve(shardId, generation);
        if (binding == null) {
            logger.debug("no Parquet binding for shard {} generation {}", shardId, generation);
            return null;
        }
        long storePointer = binding.storePointer();
        Path path = binding.parquetFile();
        if (storePointer == ParquetColumnReader.LOCAL_STORE && Files.exists(path) == false) {
            return null;
        }
        return new ParquetSource(path, storePointer);
    }
}
