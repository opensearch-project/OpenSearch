/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesConsumerWrapper;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

import static org.opensearch.index.codec.composite.composite99.Composite99Codec.COMPOSITE_INDEX_CODEC_NAME;

/**
 * A factory class that provides a factory method for creating {@link DocValuesConsumer} instances
 * based on the specified composite codec.
 *
 * @opensearch.experimental
 */
public class LuceneDocValuesConsumerFactory {

    public static DocValuesConsumer getDocValuesConsumerForCompositeCodec(
        String compositeCodec,
        SegmentWriteState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {

        switch (compositeCodec) {
            case COMPOSITE_INDEX_CODEC_NAME:
                return new Lucene90DocValuesConsumerWrapper(state, dataCodec, dataExtension, metaCodec, metaExtension)
                    .getLucene90DocValuesConsumer();
            default:
                throw new IllegalStateException("Invalid composite codec " + "[" + compositeCodec + "]");
        }

    }

}
