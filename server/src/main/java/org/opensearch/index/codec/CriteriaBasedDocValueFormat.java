/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * Doc value format for context aware enabled indices. This is used to attach bucket attributes to field info for
 * segments of child level writers. This is used to ensure attributes remain intact during segment merges.
 *
 * @opensearch.internal
 */
public class CriteriaBasedDocValueFormat extends DocValuesFormat {

    private final DocValuesFormat delegate;
    private final String bucket;

    public CriteriaBasedDocValueFormat() {
        this(new Lucene90DocValuesFormat(), null);
    }

    public CriteriaBasedDocValueFormat(String bucket) {
        this(new Lucene90DocValuesFormat(), bucket);
    }

    /**
     * Creates a new docvalues format.
     *
     */
    protected CriteriaBasedDocValueFormat(DocValuesFormat delegate, String bucket) {
        super(delegate.getName());
        this.delegate = delegate;
        this.bucket = bucket;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        return new CriteriaBasedDocValuesWriter(delegate.fieldsConsumer(state), bucket);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }
}
