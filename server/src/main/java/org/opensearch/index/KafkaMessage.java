/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.index.engine.Engine;

public class KafkaMessage implements Message<byte[]> {
    // TODO: support kafka header
    private final byte[] key;
    private final byte[] payload;

    public KafkaMessage(byte[] key, byte[] payload) {
        this.key = key;
        this.payload = payload;
    }


    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getPayload() {
        return payload;
    }

    @Override
    public Engine.Operation getOperation() {
        // TODO: decode the bytes to get the operation
        throw new UnsupportedOperationException();
    }
}
