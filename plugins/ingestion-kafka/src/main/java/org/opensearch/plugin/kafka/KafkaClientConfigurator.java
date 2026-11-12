/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Properties;

/**
 * Adjusts the properties of the Kafka clients a pull-based index uses, before each client is created. Implementations
 * come from plugins that extend {@code ingestion-kafka}: they declare {@code extendedPlugins = ['ingestion-kafka']} and
 * list their implementation in {@code META-INF/services/org.opensearch.plugin.kafka.KafkaClientConfigurator}.
 * Implementations that read files or other protected resources do so in a privileged block under their own plugin's
 * security policy.
 */
@ExperimentalApi
public interface KafkaClientConfigurator {

    /**
     * The kind of Kafka client being created.
     */
    @ExperimentalApi
    enum ClientType {
        /** A consumer that reads a shard's partition. */
        CONSUMER,
        /** A producer that writes to the index's stream. */
        PRODUCER
    }

    /**
     * Adds or changes Kafka client properties for an index. Called after the properties from the index's
     * {@code index.ingestion_source.param.*} settings are applied, so values set here take precedence.
     *
     * @param indexMetadata the pull-based index
     * @param clientType    the kind of client being created
     * @param properties    the Kafka client properties, changed in place
     */
    void configure(IndexMetadata indexMetadata, ClientType clientType, Properties properties);
}
