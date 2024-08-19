/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.opensearch.index.codec.composite.CompositeDocValuesProducer;

import java.io.IOException;

/**
 * This class is a custom abstraction of the {@link DocValuesProducer} for the Star Tree index structure.
 * It is responsible for providing access to various types of document values (numeric, binary, sorted, sorted numeric,
 * and sorted set) for fields in the Star Tree index.
 *
 * @opensearch.experimental
 */
public class Lucene90DocValuesProducerWrapper implements CompositeDocValuesProducer {

    private final Lucene90DocValuesProducer lucene90DocValuesProducer;

    public Lucene90DocValuesProducerWrapper(
        SegmentReadState state,
        String dataCodec,
        String dataExtension,
        String metaCodec,
        String metaExtension
    ) throws IOException {
        lucene90DocValuesProducer = new Lucene90DocValuesProducer(state, dataCodec, dataExtension, metaCodec, metaExtension);
    }

    @Override
    public DocValuesProducer getDocValuesProducer() {
        return lucene90DocValuesProducer;
    }

    @Override
    public void close() throws IOException {
        lucene90DocValuesProducer.close();
    }
}
