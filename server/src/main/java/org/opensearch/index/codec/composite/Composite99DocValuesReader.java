/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.List;

/**
 * Reader for star tree index and star tree doc values from the segments
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Composite99DocValuesReader extends DocValuesProducer implements CompositeIndexReader {
    private DocValuesProducer delegate;

    public Composite99DocValuesReader(DocValuesProducer producer, SegmentReadState state) throws IOException {
        this.delegate = producer;
        // TODO : read star tree files
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return delegate.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return delegate.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return delegate.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return delegate.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return delegate.getSortedSet(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
        // Todo : check integrity of composite index related [star tree] files
    }

    @Override
    public void close() throws IOException {
        delegate.close();
        // Todo: close composite index related files [star tree] files
    }

    @Override
    public List<String> getCompositeIndexFields() {
        // todo : read from file formats and get the field names.
        throw new UnsupportedOperationException();

    }

    @Override
    public CompositeIndexValues getCompositeIndexValues(String field, CompositeMappedFieldType.CompositeFieldType fieldType)
        throws IOException {
        // TODO : read compositeIndexValues [starTreeValues] from star tree files
        throw new UnsupportedOperationException();
    }
}
