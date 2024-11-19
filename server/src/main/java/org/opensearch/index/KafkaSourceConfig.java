/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

public class KafkaSourceConfig implements IngestionSourceConfig  {
    private final String topic;
    private final String clientId;
    private final String groupId;
    private final String bootstrapServers;
    private final int partition;

    public static final ConstructingObjectParser<KafkaSourceConfig, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        true,
        a -> new KafkaSourceConfig((String) a[0], (String) a[1], (String) a[2], (String) a[3], (int) a[4])
    );

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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("topic", topic);
        builder.field("clientId", clientId);
        builder.field("groupId", groupId);
        builder.field("bootstrapServers", bootstrapServers);
        builder.endObject();
        return builder;
    }

    @Override
    public String getIngestionSourceType() {
        return "KAFKA";
    }

    public static KafkaSourceConfig fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
