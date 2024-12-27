/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;


import java.util.Map;
import java.util.Objects;

/**
 * Class encapsulating the configuration of a Kafka source.
 */
public class KafkaSourceConfig {
    private final String topic;
    private final String groupId;
    private final String bootstrapServers;

    public KafkaSourceConfig(Map<String, Object> params) {
        // TODO: better parsing and validation
        this.topic = (String) Objects.requireNonNull(params.get("topic"));
        this.groupId = (String) params.get("groupId");
        this.bootstrapServers = (String) Objects.requireNonNull(params.get("bootstrapServers"));
        assert this.bootstrapServers != null;
    }

    /**
     * @return the topic name
     */
    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    /**
     * @return the bootstrap servers
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }
}
