/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * An interface for the pointer to a shard in the ingestion engine, and it is used to track the message offset of
 * ingestion.
 */
@ExperimentalApi
public interface IngestionShardPointer extends Comparable<IngestionShardPointer> {

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
}
