/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.Message;

public class KafkaMessage implements Message<String> {
    // TODO: support kafka header
    private final String key;
    private final String payload;
    // FIXME: how to make this generic outside of kafka message?
    private final KafkaOffset offset;

    public KafkaMessage(String key, String payload, KafkaOffset offset) {
        this.key = key;
        this.payload = payload;
        this.offset = offset;
    }


    public String getKey() {
        return key;
    }

    @Override
    public String getPayload() {
        return payload;
    }
}
