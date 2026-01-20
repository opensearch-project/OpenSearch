/*Add commentMore actions
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;

/**
 * Filter codec used to attach bucket attributes to segments of child writer.
 *
 */
public class CriteriaBasedCodec extends FilterCodec {

    private final String bucket;
    public static final String BUCKET_NAME = "bucket";
    public static final String ATTRIBUTE_BINDING_TARGET_FIELD = "_id";

    public CriteriaBasedCodec(Codec delegate, String bucket) {
        super(delegate.getName(), delegate);
        this.bucket = bucket;
    }

    @Override
    public PostingsFormat postingsFormat() {
        PostingsFormat format = super.postingsFormat();
        if (format instanceof PerFieldPostingsFormat) {
            return new PerFieldPostingsFormat() {

                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    if (field.equals(ATTRIBUTE_BINDING_TARGET_FIELD)) {
                        return new CriteriaBasedPostingsFormat(((PerFieldPostingsFormat) format).getPostingsFormatForField(field), bucket);
                    } else {
                        return ((PerFieldPostingsFormat) format).getPostingsFormatForField(field);
                    }
                }
            };
        }
        return format;
    }
}
