/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * This class is an abstraction of the {@link DocValuesConsumer} for the Star Tree index structure.
 * It is responsible to consume various types of document values (numeric, binary, sorted, sorted numeric,
 * and sorted set) for fields in the Star Tree index.
 *
 * @opensearch.experimental
 */
public class Lucene90DocValuesConsumerWrapper {

    private final Lucene90DocValuesConsumer lucene90DocValuesConsumer;

    public Lucene90DocValuesConsumerWrapper(
        SegmentWriteState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        lucene90DocValuesConsumer = new Lucene90DocValuesConsumer(state, dataCodec, dataExtension, metaCodec, metaExtension);
    }

    public Lucene90DocValuesConsumer getLucene90DocValuesConsumer() {
        return lucene90DocValuesConsumer;
    }
}
