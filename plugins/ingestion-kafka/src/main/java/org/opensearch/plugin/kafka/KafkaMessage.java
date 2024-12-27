/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.Message;

/**
 * Kafka message
 */
public class KafkaMessage implements Message<String> {
    private final String key;
    private final String payload;

    public KafkaMessage(String key, String payload) {
        this.key = key;
        this.payload = payload;
    }


    /**
     * Get the key of the message
     * @return the key of the message
     */
    public String getKey() {
        return key;
    }

    @Override
    public String getPayload() {
        return payload;
    }
}
