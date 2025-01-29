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
    private final String bootstrapServers;

    /**
     * Constructor
     * @param params the configuration parameters
     */
    public KafkaSourceConfig(Map<String, Object> params) {
        // TODO: better parsing and validation
        this.topic = (String) Objects.requireNonNull(params.get("topic"));
        this.bootstrapServers = (String) Objects.requireNonNull(params.get("bootstrap_servers"));
        assert this.bootstrapServers != null;
    }

    /**
     * Get the topic name
     * @return the topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Get the bootstrap servers
     *
     * @return the bootstrap servers
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }
}
