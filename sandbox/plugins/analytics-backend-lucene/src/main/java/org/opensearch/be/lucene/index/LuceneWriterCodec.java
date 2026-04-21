/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.engine.dataformat.FlushInput;

import java.io.IOException;

/**
 * A {@link FilterCodec} wrapper that injects the {@code writer_generation} attribute into
 * every segment written by a {@link LuceneWriter}.
 * <p>
 * During {@link LuceneWriter#flush(FlushInput)}, each per-generation writer creates exactly one segment.
 * This codec intercepts the {@link SegmentInfoFormat#write} call to stamp the segment with
 * the writer generation number. After the segment is incorporated into the shared
 * {@link org.apache.lucene.index.IndexWriter} via {@code addIndexes}, the attribute allows
 * {@link LuceneIndexingExecutionEngine} to correlate the Lucene segment back to the
 * originating writer generation and its corresponding Parquet file.
 *
 * @opensearch.experimental
 */
public class LuceneWriterCodec extends FilterCodec {

    private final long writerGeneration;

    /**
     * Creates a new codec that wraps the given delegate and tags segments with the specified
     * writer generation.
     *
     * @param delegate          the underlying codec to delegate all format operations to
     * @param writerGeneration  the generation number to store as a segment info attribute
     */
    public LuceneWriterCodec(Codec delegate, long writerGeneration) {
        super(delegate.getName(), delegate);
        this.writerGeneration = writerGeneration;
    }

    /**
     * Returns a {@link SegmentInfoFormat} that delegates reads to the underlying codec and
     * intercepts writes to inject the {@code writer_generation} attribute into the
     * {@link SegmentInfo} before persisting.
     *
     * @return the decorated segment info format
     */
    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                info.putAttribute("writer_generation", String.valueOf(writerGeneration));
                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }
}
