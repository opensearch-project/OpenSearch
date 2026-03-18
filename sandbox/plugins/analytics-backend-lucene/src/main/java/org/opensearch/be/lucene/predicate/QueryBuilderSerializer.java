/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.predicate;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;

/**
 * Handles round-trip serialization of {@link QueryBuilder} to/from {@code byte[]}
 * using OpenSearch's {@link NamedWriteableRegistry} infrastructure.
 *
 * <p>The registry entries are derived from {@link PredicateHandlerRegistry},
 * so adding a new predicate handler automatically makes its QueryBuilder
 * types serializable — no changes needed here.
 */
public class QueryBuilderSerializer {

    private static final NamedWriteableRegistry REGISTRY = new NamedWriteableRegistry(
        PredicateHandlerRegistry.allNamedWriteableEntries()
    );

    /**
     * Serializes a {@link QueryBuilder} into a byte array.
     *
     * @param queryBuilder the query builder to serialize
     * @return the serialized byte array
     * @throws IllegalArgumentException if serialization fails
     */
    public static byte[] serialize(QueryBuilder queryBuilder) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(queryBuilder);
            return BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to serialize QueryBuilder: " + e.getMessage(), e);
        }
    }

    /**
     * Deserializes a byte array back into a {@link QueryBuilder}.
     *
     * @param data the byte array to deserialize
     * @return the deserialized QueryBuilder
     * @throws IllegalArgumentException if the byte array is null, empty, or corrupted
     */
    public static QueryBuilder deserialize(byte[] data) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Fragment byte array must not be null or empty");
        }
        try (StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), REGISTRY)) {
            return in.readNamedWriteable(QueryBuilder.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to deserialize QueryBuilder: " + e.getMessage(), e);
        }
    }
}
