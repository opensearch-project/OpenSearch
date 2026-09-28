/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.MergeState;

import java.io.IOException;

/**
 * This is used to attach bucket attributes to seq_no field info for segments of child level writers. This is used to
 * ensure attributes remain intact during segment merges.
 *
 * @opensearch.internal
 */
public class CriteriaBasedDocValuesWriter extends DocValuesConsumer {

    private final DocValuesConsumer delegate;
    private final String bucket;

    public CriteriaBasedDocValuesWriter(DocValuesConsumer delegate, String bucket) throws IOException {
        this.delegate = delegate;
        this.bucket = bucket;
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
        if (field.name.equals("_seq_no") && bucket != null) {
            field.putAttribute(CriteriaBasedCodec.BUCKET_NAME, bucket);
        }
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addBinaryField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }

    @Override
    public void merge(MergeState mergeState) throws IOException {
        super.merge(mergeState);
        mergeState.segmentInfo.putAttribute(
            CriteriaBasedCodec.BUCKET_NAME,
            mergeState.mergeFieldInfos.fieldInfo("_seq_no").getAttribute(CriteriaBasedCodec.BUCKET_NAME)
        );
        mergeState.segmentInfo.putAttribute("merge", "true");
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
