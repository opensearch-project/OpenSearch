/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Factory class to create fuzzy set.
 * Supports bloom filters for now. More sets can be added as required.
 */
public class FuzzySetFactory {

    private final Map<String, FuzzySetParameters> setTypeForField;

    public FuzzySetFactory(Map<String, FuzzySetParameters> setTypeForField) {
        this.setTypeForField = setTypeForField;
    }

    public FuzzySet createFuzzySet(int maxDocs, String fieldName, CheckedSupplier<Iterator<BytesRef>, IOException> iteratorProvider)
        throws IOException {
        FuzzySetParameters params = setTypeForField.get(fieldName);
        if (params == null) {
            throw new IllegalArgumentException("No fuzzy set defined for field: " + fieldName);
        }
        switch (params.getSetType()) {
            case BLOOM_FILTER_V1:
                return new BloomFilter(maxDocs, params.getFalsePositiveProbability(), iteratorProvider);
            default:
                throw new IllegalArgumentException("No Implementation for set type: " + params.getSetType());
        }
    }

    public static FuzzySet deserializeFuzzySet(IndexInput in) throws IOException {
        FuzzySet.SetType setType = FuzzySet.SetType.from(in.readString());
        return setType.getDeserializer().apply(in);
    }
}
