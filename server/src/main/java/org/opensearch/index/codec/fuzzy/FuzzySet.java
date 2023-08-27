/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedFunction;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;

/**
 * Fuzzy Filter interface
 */
public interface FuzzySet extends Accountable {

    /**
     * Name used for a codec to be aware of what fuzzy set has been used.
     */
    SetType setType();

    /**
     * @param value the item whose membership needs to be checked.
     * @return Result see {@Result}
     */
    Result contains(BytesRef value);

    /**
     * Add an item to this fuzzy set.
     * @param value The value to be added
     */
    void add(BytesRef value);

    /**
     * Add all items to the underlying set.
     * Implementations can choose to perform this using an optimized strategy based on the type of set.
     * @param values All values which should be added to the set.
     */
    default void addAll(Iterable<BytesRef> values) {
        for (BytesRef val: values) {
            add(val);
        }
    }

    boolean isSaturated();

    Optional<FuzzySet> maybeDownsize();

    void writeTo(DataOutput out) throws IOException;

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

    enum SetType {
        BLOOM_FILTER_V1("bloom_filter_v1", BloomFilter::new);

        private String setName;
        private CheckedFunction<DataInput, ? extends FuzzySet, IOException> deserializer;

        SetType(String setName,  CheckedFunction<DataInput, ? extends FuzzySet, IOException> deserializer) {
            this.setName = setName;
            this.deserializer = deserializer;
        }

        public String getSetName() {
            return setName;
        }

        public CheckedFunction<DataInput, ? extends FuzzySet, IOException> getDeserializer() {
            return deserializer;
        }

        public static SetType from(String name) {
            for (SetType type: SetType.values()) {
                if (type.setName.equals(name)) {
                    return type;
                }
            }
            throw new IllegalArgumentException("There is no implementation for fuzzy set: " + name);
        }
    }
}
