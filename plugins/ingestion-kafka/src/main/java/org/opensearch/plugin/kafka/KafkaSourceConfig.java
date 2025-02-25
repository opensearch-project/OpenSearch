/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.core.util.ConfigurationUtils;

import java.util.Map;

/**
 * Class encapsulating the configuration of a Kafka source.
 */
public class KafkaSourceConfig {
    private final String PROP_TOPIC = "topic";
    private final String PROP_BOOTSTRAP_SERVERS = "bootstrap_servers";
    // TODO: support pass any generic kafka configs
    private final String PROP_AUTO_OFFSET_RESET = "auto.offset.reset";

    private final String topic;
    private final String bootstrapServers;
    private final String autoOffsetResetConfig;

    /**
     * Constructor
     * @param params the configuration parameters
     */
    public KafkaSourceConfig(Map<String, Object> params) {
        this.topic = ConfigurationUtils.readStringProperty(params, PROP_TOPIC);
        this.bootstrapServers = ConfigurationUtils.readStringProperty(params, PROP_BOOTSTRAP_SERVERS);
        this.autoOffsetResetConfig = ConfigurationUtils.readOptionalStringProperty(params, PROP_AUTO_OFFSET_RESET);
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

    /**
     * Get the auto offset reset configuration
     *
     * @return the auto offset reset configuration
     */
    public String getAutoOffsetResetConfig() {
        return autoOffsetResetConfig;
    }
}
