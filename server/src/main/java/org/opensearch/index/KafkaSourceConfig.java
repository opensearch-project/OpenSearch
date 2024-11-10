/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

public class KafkaSourceConfig implements IngestionSourceConfig {
    private final String topic;
    private final String clientId;
    private final String groupId;
    private final String bootstrapServers;
    private final int partition;

    public KafkaSourceConfig(String topic, String clientId, String groupId, String bootstrapServers, int partition) {
        this.topic = topic;
        this.clientId = clientId;
        this.groupId = groupId;
        this.bootstrapServers = bootstrapServers;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public String getClientId() {
        return clientId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public int getPartition() {
        return partition;
    }
}
