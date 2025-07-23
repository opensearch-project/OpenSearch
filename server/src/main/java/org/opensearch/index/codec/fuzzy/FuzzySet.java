/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.CheckedSupplier;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

/**
 * Fuzzy Filter interface
 */
public interface FuzzySet<T extends FuzzySet.Meta> extends Accountable, Closeable {

    /**
     * Name used for a codec to be aware of what fuzzy set has been used.
     */
    SetType setType();

    /**
     * @param value the item whose membership needs to be checked.
     */
    Result contains(BytesRef value);

    boolean isSaturated();

    void writeTo(DataOutput out) throws IOException;

    /**
     * Enum to represent result of membership check on a fuzzy set.
     */
    enum Result {
        /**
         * A definite no for the set membership of an item.
         */
        NO,

        /**
         * Fuzzy sets cannot guarantee that a given item is present in the set or not due the data being stored in
         * a lossy format (e.g. fingerprint, hash).
         * Hence, we return a response denoting that the item maybe present.
         */
        MAYBE
    }

    /**
     * Enum to declare supported properties and mappings for a fuzzy set implementation.
     */
    enum SetType {
        BLOOM_FILTER_V1("bloom_filter_v1", List.of("bloom_filter"), (in) -> {
            BloomFilter.BloomMeta meta = new BloomFilter.BloomMeta(in);
            return () -> new BloomFilter(meta);
        });

        /**
         * Name persisted in postings file. This will be used when reading to determine the bloom filter implementation.
         */
        private final String setName;

        private final CheckedFunction<IndexInput, CheckedSupplier<? extends FuzzySet, IOException>, IOException> metaExtractor;

        SetType(String setName,
                List<String> aliases,
                CheckedFunction<IndexInput, CheckedSupplier<? extends FuzzySet, IOException>, IOException> metaExtractor) {
            if (aliases.size() < 1) {
                throw new IllegalArgumentException("Alias list is empty. Could not create Set Type: " + setName);
            }
            this.setName = setName;
            this.metaExtractor = metaExtractor;
        }

        public String getSetName() {
            return setName;
        }

        public CheckedSupplier<? extends FuzzySet, IOException> extractMetaAndGetSupplier(IndexInput in) throws IOException {
            return metaExtractor.apply(in);
        }

        public static SetType from(String name) {
            for (SetType type : SetType.values()) {
                if (type.setName.equals(name)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("There is no implementation for fuzzy set: " + name);
        }
    }

    interface Meta {

    }
}
