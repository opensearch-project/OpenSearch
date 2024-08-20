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

/**
 * A factory class that provides a factory method for creating {@link DocValuesConsumer} instances
 * for the latest composite codec.
 * <p>
 * The segments are written using the latest composite codec. The codec
 * internally manages calling the appropriate consumer factory for its abstractions.
 * <p>
 * This design ensures forward compatibility for writing operations
 *
 * @opensearch.experimental
 */
public class LuceneDocValuesConsumerFactory {

    public static DocValuesConsumer getDocValuesConsumerForCompositeCodec(
        SegmentWriteState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        try (
            Lucene90DocValuesConsumerWrapper lucene90DocValuesConsumerWrapper = new Lucene90DocValuesConsumerWrapper(
                state,
                dataCodec,
                dataExtension,
                metaCodec,
                metaExtension
            )
        ) {
            return lucene90DocValuesConsumerWrapper.getLucene90DocValuesConsumer();
        }
    }

}
