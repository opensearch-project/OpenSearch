/*Add commentMore actions
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;

/**
 * Filter codec used to attach bucket attributes to segments of child writer.
 *
 */
public class CriteriaBasedCodec extends FilterCodec {

    private final String bucket;
    public static final String BUCKET_NAME = "bucket";
    private static final String PLACEHOLDER_BUCKET_FOR_PARENT_WRITER = "-2";

    public CriteriaBasedCodec() {
        super("CriteriaBasedCodec", new Lucene103Codec());
        bucket = null;
    }

    public CriteriaBasedCodec(Codec delegate, String bucket) {
        super("CriteriaBasedCodec", delegate);
        this.bucket = bucket;
    }

    @Override
    public SegmentInfoFormat segmentInfoFormat() {
        return new SegmentInfoFormat() {
            @Override
            public SegmentInfo read(Directory directory, String segmentName, byte[] segmentID, IOContext context) throws IOException {
                return delegate.segmentInfoFormat().read(directory, segmentName, segmentID, context);
            }

            @Override
            public void write(Directory directory, SegmentInfo info, IOContext ioContext) throws IOException {
                if (bucket != null) {
                    // We will set BUCKET_NAME attribute only for child writer where bucket will set.
                    info.putAttribute(BUCKET_NAME, bucket);
                } else if (info.getAttribute(BUCKET_NAME) == null) {
                    // For segment belonging to parent writer, attributes will be set. In case write went to parent
                    // writer (like for no ops writes or for temporary tombstone entry which is added for deletes/updates
                    // to sync version across child and parent writers), segments corresponding to those writer does not
                    // have
                    info.putAttribute(BUCKET_NAME, PLACEHOLDER_BUCKET_FOR_PARENT_WRITER);
                }

                delegate.segmentInfoFormat().write(directory, info, ioContext);
            }
        };
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return new CriteriaBasedDocValueFormat(bucket);
    }
}
