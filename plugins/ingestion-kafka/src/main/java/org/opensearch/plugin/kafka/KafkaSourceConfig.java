/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.opensearch.core.util.ConfigurationUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Class encapsulating the configuration of a Kafka source.
 */
public class KafkaSourceConfig {
    private final String PROP_TOPIC = "topic";
    private final String PROP_BOOTSTRAP_SERVERS = "bootstrap_servers";

    private final String topic;
    private final String bootstrapServers;
    private final String autoOffsetResetConfig;
    private final int maxPollRecords;

    private final Map<String, Object> consumerConfigsMap;

    /**
     * Extracts and look for required and optional kafka consumer configurations.
     * @param maxPollSize the maximum batch size to read in a single poll
     * @param params the configuration parameters
     */
    public KafkaSourceConfig(int maxPollSize, Map<String, Object> params) {
        this.consumerConfigsMap = new HashMap<>(params);
        this.topic = ConfigurationUtils.readStringProperty(params, PROP_TOPIC);
        this.bootstrapServers = ConfigurationUtils.readStringProperty(params, PROP_BOOTSTRAP_SERVERS);

        // 'auto.offset.reset' is handled differently for Kafka sources, with the default set to none.
        // This ensures out-of-bounds offsets throw an error, unless the user explicitly sets different value.
        this.autoOffsetResetConfig = ConfigurationUtils.readStringProperty(params, ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        // OpenSearch supports 'maxPollSize' setting for consumers. If user did not provide a 'max.poll.records' setting,
        // maxPollSize will be used instead.
        this.maxPollRecords = ConfigurationUtils.readIntProperty(params, ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollSize);

        // remove metadata configurations
        consumerConfigsMap.remove(PROP_TOPIC);
        consumerConfigsMap.remove(PROP_BOOTSTRAP_SERVERS);

        // add or overwrite required configurations with defaults if not present
        consumerConfigsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);
        consumerConfigsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
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

    public Map<String, Object> getConsumerConfigurations() {
        return consumerConfigsMap;
    }
}
