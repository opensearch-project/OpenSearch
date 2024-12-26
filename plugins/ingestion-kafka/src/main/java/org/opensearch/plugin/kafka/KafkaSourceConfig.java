/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.kafka;

import org.opensearch.index.IngestionSourceConfig;

import java.util.Map;

public class KafkaSourceConfig implements IngestionSourceConfig {
    private final String topic;
    private final String groupId;
    private final String bootstrapServers;

    public KafkaSourceConfig(Map<String, Object> params) {
        this.topic = (String) params.get("topic");
        assert this.topic != null;
        this.groupId = (String) params.get("groupId");
        this.bootstrapServers = (String) params.get("bootstrapServers");
        assert this.bootstrapServers != null;
    }

    public String getTopic() {
        return topic;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public String getIngestionSourceType() {
        return "kafka";
    }
}
