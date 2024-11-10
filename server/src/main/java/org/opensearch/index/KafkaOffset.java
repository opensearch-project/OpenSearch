/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

public class KafkaOffset implements IngestionShardPointer {

    private final long offset;

    public KafkaOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public IngestionShardPointer deserialize(byte[] serialized) {
        return null;
    }


    public long getOffset() {
        return offset;
    }
}
