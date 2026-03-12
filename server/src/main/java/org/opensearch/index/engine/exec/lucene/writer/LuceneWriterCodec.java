/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

public class LuceneWriterCodec extends FilterCodec {

    private final long writerGeneration;

    /**
     * Sole constructor. When subclassing this codec, create a no-arg ctor and pass the delegate codec
     * and a unique name to this ctor.
     *
     * @param name
     * @param delegate
     */
    public LuceneWriterCodec(String name, Codec delegate, long writerGeneration) {
        super(name, delegate);
        this.writerGeneration = writerGeneration;
    }

    // TODO:: Why this? What does it do?
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
