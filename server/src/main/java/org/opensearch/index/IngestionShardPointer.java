/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * An interface for the pointer to a shard in the ingestion engine, and it is used to track the message offset of
 * ingestion.
 */
@ExperimentalApi
public interface IngestionShardPointer extends Comparable<IngestionShardPointer> {
    String OFFSET_FIELD = "_offset";

    /**
     * Serialize the pointer to a byte array.
     * @return the serialized byte array
     */
    byte[] serialize();

    /**
     * Convert the pointer to a string.
     * @return the string representation of the pointer
     */
    String asString();

    /**
     * Creates a point field for this pointer. This is used to store the pointer in the index for range search during
     * checkpoint recovery.
     * @param fieldName the field name to create the point field
     * @return the point field
     */
    Field asPointField(String fieldName);

    /**
     * Create a new range query for values greater than the pointer. This is used in recovering from the ingestion
     * checkpoints.
     *
     * @param fieldName the field name to create the range query
     * @return query for values greater than the pointer
     */
    Query newRangeQueryGreaterThan(String fieldName);
}
