/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import com.jcraft.jzlib.JZlib;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Stores checksum for all components in cluster state. This will be used to ensure cluster state is same across all nodes in the cluster.
 */
public class ClusterStateChecksum implements ToXContentFragment, Writeable {

    private static final String ROUTING_TABLE_CS = "routing_table";
    private static final String NODES_CS = "discovery_nodes";
    private static final String BLOCKS_CS = "blocks";
    private static final String CUSTOMS_CS = "customs";
    private static final String COORDINATION_MD_CS = "coordination_md";
    private static final String SETTINGS_MD_CS = "settings_md";
    private static final String TRANSIENT_SETTINGS_MD_CS = "transient_settings_md";
    private static final String TEMPLATES_MD_CS = "templated_md";
    private static final String CUSTOM_MD_CS = "customs_md";
    private static final String HASHES_MD_CS = "hashes_md";
    private static final String INDICES_CS = "indices_md";
    private static final String CLUSTER_STATE_CS = "cluster_state";
    private static final Logger logger = LogManager.getLogger(ClusterStateChecksum.class);

    long routingTableChecksum;
    long nodesChecksum;
    long blocksChecksum;
    long clusterStateCustomsChecksum;
    long coordinationMetadataChecksum;
    long settingMetadataChecksum;
    long transientSettingsMetadataChecksum;
    long templatesMetadataChecksum;
    long customMetadataMapChecksum;
    long hashesOfConsistentSettingsChecksum;
    long indicesChecksum;
    long clusterStateChecksum;

    public ClusterStateChecksum(ClusterState clusterState) {
        try (
            BytesStreamOutput out = new BytesStreamOutput();
            BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out)
        ) {
            clusterState.routingTable().writeTo(checksumOut);
            routingTableChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.nodes().writeTo(checksumOut);
            nodesChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.coordinationMetadata().writeTo(checksumOut);
            coordinationMetadataChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            Settings.writeSettingsToStream(clusterState.metadata().persistentSettings(), checksumOut);
            settingMetadataChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            Settings.writeSettingsToStream(clusterState.metadata().transientSettings(), checksumOut);
            transientSettingsMetadataChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.metadata().templatesMetadata().writeTo(checksumOut);
            templatesMetadataChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.metadata().customs().forEach((key, custom) -> {
                try {
                    custom.writeTo(checksumOut);
                } catch (IOException e) {
                    logger.error("Failed to create checksum for custom metadata.", e);
                    throw new RemoteStateTransferException("Failed to create checksum for custom metadata.", e);
                }
            });
            customMetadataMapChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            ((DiffableStringMap) clusterState.metadata().hashesOfConsistentSettings()).writeTo(checksumOut);
            hashesOfConsistentSettingsChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.metadata().indices().forEach(((s, indexMetadata) -> {
                try {
                    indexMetadata.writeTo(checksumOut);
                } catch (IOException e) {
                    logger.error("Failed to create checksum for index metadata.", e);
                    throw new RemoteStateTransferException("Failed to create checksum for index metadata.", e);
                }
            }));
            indicesChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.blocks().writeTo(checksumOut);
            blocksChecksum = checksumOut.getChecksum();

            checksumOut.reset();
            clusterState.customs().forEach((key, value) -> {
                try {
                    checksumOut.writeNamedWriteable(value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            clusterStateCustomsChecksum = checksumOut.getChecksum();
        } catch (IOException e) {
            logger.error("Failed to create checksum for cluster state.", e);
            throw new RemoteStateTransferException("Failed to create checksum for cluster state.", e);
        }
        clusterStateChecksum = JZlib.crc32_combine(routingTableChecksum, nodesChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, blocksChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, clusterStateCustomsChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, coordinationMetadataChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, settingMetadataChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, transientSettingsMetadataChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, templatesMetadataChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, customMetadataMapChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, hashesOfConsistentSettingsChecksum, 8);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, indicesChecksum, 8);
    }

    public static ClusterStateChecksum.Builder builder() {
        return new ClusterStateChecksum.Builder();
    }

    public ClusterStateChecksum(
        long routingTableChecksum,
        long nodesChecksum,
        long blocksChecksum,
        long clusterStateCustomsChecksum,
        long coordinationMetadataChecksum,
        long settingMetadataChecksum,
        long transientSettingsMetadataChecksum,
        long templatesMetadataChecksum,
        long customMetadataMapChecksum,
        long hashesOfConsistentSettingsChecksum,
        long indicesChecksum,
        long clusterStateChecksum
    ) {
        this.routingTableChecksum = routingTableChecksum;
        this.nodesChecksum = nodesChecksum;
        this.blocksChecksum = blocksChecksum;
        this.clusterStateCustomsChecksum = clusterStateCustomsChecksum;
        this.coordinationMetadataChecksum = coordinationMetadataChecksum;
        this.settingMetadataChecksum = settingMetadataChecksum;
        this.transientSettingsMetadataChecksum = transientSettingsMetadataChecksum;
        this.templatesMetadataChecksum = templatesMetadataChecksum;
        this.customMetadataMapChecksum = customMetadataMapChecksum;
        this.hashesOfConsistentSettingsChecksum = hashesOfConsistentSettingsChecksum;
        this.indicesChecksum = indicesChecksum;
        this.clusterStateChecksum = clusterStateChecksum;
    }

    public ClusterStateChecksum(StreamInput in) throws IOException {
        routingTableChecksum = in.readLong();
        nodesChecksum = in.readLong();
        blocksChecksum = in.readLong();
        clusterStateCustomsChecksum = in.readLong();
        coordinationMetadataChecksum = in.readLong();
        settingMetadataChecksum = in.readLong();
        transientSettingsMetadataChecksum = in.readLong();
        templatesMetadataChecksum = in.readLong();
        customMetadataMapChecksum = in.readLong();
        hashesOfConsistentSettingsChecksum = in.readLong();
        indicesChecksum = in.readLong();
        clusterStateChecksum = in.readLong();
    }

    public static ClusterStateChecksum fromXContent(XContentParser parser) throws IOException {
        ClusterStateChecksum.Builder builder = new ClusterStateChecksum.Builder();
        if (parser.currentToken() == null) { // fresh parser? move to next token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        XContentParser.Token token;
        String currentFieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case ROUTING_TABLE_CS:
                        builder.routingTableChecksum(parser.longValue());
                        break;
                    case NODES_CS:
                        builder.nodesChecksum(parser.longValue());
                        break;
                    case BLOCKS_CS:
                        builder.blocksChecksum(parser.longValue());
                        break;
                    case CUSTOMS_CS:
                        builder.clusterStateCustomsChecksum(parser.longValue());
                        break;
                    case COORDINATION_MD_CS:
                        builder.coordinationMetadataChecksum(parser.longValue());
                        break;
                    case SETTINGS_MD_CS:
                        builder.settingMetadataChecksum(parser.longValue());
                        break;
                    case TRANSIENT_SETTINGS_MD_CS:
                        builder.transientSettingsMetadataChecksum(parser.longValue());
                        break;
                    case TEMPLATES_MD_CS:
                        builder.templatesMetadataChecksum(parser.longValue());
                        break;
                    case CUSTOM_MD_CS:
                        builder.customMetadataMapChecksum(parser.longValue());
                        break;
                    case HASHES_MD_CS:
                        builder.hashesOfConsistentSettingsChecksum(parser.longValue());
                        break;
                    case INDICES_CS:
                        builder.indicesChecksum(parser.longValue());
                        break;
                    case CLUSTER_STATE_CS:
                        builder.clusterStateChecksum(parser.longValue());
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new XContentParseException("Unexpected token [" + token + "]");
            }
        }
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(routingTableChecksum);
        out.writeLong(nodesChecksum);
        out.writeLong(blocksChecksum);
        out.writeLong(clusterStateCustomsChecksum);
        out.writeLong(coordinationMetadataChecksum);
        out.writeLong(settingMetadataChecksum);
        out.writeLong(transientSettingsMetadataChecksum);
        out.writeLong(templatesMetadataChecksum);
        out.writeLong(customMetadataMapChecksum);
        out.writeLong(hashesOfConsistentSettingsChecksum);
        out.writeLong(indicesChecksum);
        out.writeLong(clusterStateChecksum);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(ROUTING_TABLE_CS, routingTableChecksum);
        builder.field(NODES_CS, nodesChecksum);
        builder.field(BLOCKS_CS, blocksChecksum);
        builder.field(CUSTOMS_CS, clusterStateCustomsChecksum);
        builder.field(COORDINATION_MD_CS, coordinationMetadataChecksum);
        builder.field(SETTINGS_MD_CS, settingMetadataChecksum);
        builder.field(TRANSIENT_SETTINGS_MD_CS, transientSettingsMetadataChecksum);
        builder.field(TEMPLATES_MD_CS, templatesMetadataChecksum);
        builder.field(CUSTOM_MD_CS, customMetadataMapChecksum);
        builder.field(HASHES_MD_CS, hashesOfConsistentSettingsChecksum);
        builder.field(INDICES_CS, indicesChecksum);
        builder.field(CLUSTER_STATE_CS, clusterStateChecksum);
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateChecksum that = (ClusterStateChecksum) o;
        return routingTableChecksum == that.routingTableChecksum
            && nodesChecksum == that.nodesChecksum
            && blocksChecksum == that.blocksChecksum
            && clusterStateCustomsChecksum == that.clusterStateCustomsChecksum
            && coordinationMetadataChecksum == that.coordinationMetadataChecksum
            && settingMetadataChecksum == that.settingMetadataChecksum
            && transientSettingsMetadataChecksum == that.transientSettingsMetadataChecksum
            && templatesMetadataChecksum == that.templatesMetadataChecksum
            && customMetadataMapChecksum == that.customMetadataMapChecksum
            && hashesOfConsistentSettingsChecksum == that.hashesOfConsistentSettingsChecksum
            && indicesChecksum == that.indicesChecksum
            && clusterStateChecksum == that.clusterStateChecksum;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            routingTableChecksum,
            nodesChecksum,
            blocksChecksum,
            clusterStateCustomsChecksum,
            coordinationMetadataChecksum,
            settingMetadataChecksum,
            transientSettingsMetadataChecksum,
            templatesMetadataChecksum,
            customMetadataMapChecksum,
            hashesOfConsistentSettingsChecksum,
            indicesChecksum,
            clusterStateChecksum
        );
    }

    public static class Builder {
        long routingTableChecksum;
        long nodesChecksum;
        long blocksChecksum;
        long clusterStateCustomsChecksum;
        long coordinationMetadataChecksum;
        long settingMetadataChecksum;
        long transientSettingsMetadataChecksum;
        long templatesMetadataChecksum;
        long customMetadataMapChecksum;
        long hashesOfConsistentSettingsChecksum;
        long indicesChecksum;
        long clusterStateChecksum;

        public Builder routingTableChecksum(long routingTableChecksum) {
            this.routingTableChecksum = routingTableChecksum;
            return this;
        }

        public Builder nodesChecksum(long nodesChecksum) {
            this.nodesChecksum = nodesChecksum;
            return this;
        }

        public Builder blocksChecksum(long blocksChecksum) {
            this.blocksChecksum = blocksChecksum;
            return this;
        }

        public Builder clusterStateCustomsChecksum(long clusterStateCustomsChecksum) {
            this.clusterStateCustomsChecksum = clusterStateCustomsChecksum;
            return this;
        }

        public Builder coordinationMetadataChecksum(long coordinationMetadataChecksum) {
            this.coordinationMetadataChecksum = coordinationMetadataChecksum;
            return this;
        }

        public Builder settingMetadataChecksum(long settingMetadataChecksum) {
            this.settingMetadataChecksum = settingMetadataChecksum;
            return this;
        }

        public Builder transientSettingsMetadataChecksum(long transientSettingsMetadataChecksum) {
            this.transientSettingsMetadataChecksum = transientSettingsMetadataChecksum;
            return this;
        }

        public Builder templatesMetadataChecksum(long templatesMetadataChecksum) {
            this.templatesMetadataChecksum = templatesMetadataChecksum;
            return this;
        }

        public Builder customMetadataMapChecksum(long customMetadataMapChecksum) {
            this.customMetadataMapChecksum = customMetadataMapChecksum;
            return this;
        }

        public Builder hashesOfConsistentSettingsChecksum(long hashesOfConsistentSettingsChecksum) {
            this.hashesOfConsistentSettingsChecksum = hashesOfConsistentSettingsChecksum;
            return this;
        }

        public Builder indicesChecksum(long indicesChecksum) {
            this.indicesChecksum = indicesChecksum;
            return this;
        }

        public Builder clusterStateChecksum(long clusterStateChecksum) {
            this.clusterStateChecksum = clusterStateChecksum;
            return this;
        }

        public ClusterStateChecksum build() {
            return new ClusterStateChecksum(
                routingTableChecksum,
                nodesChecksum,
                blocksChecksum,
                clusterStateCustomsChecksum,
                coordinationMetadataChecksum,
                settingMetadataChecksum,
                transientSettingsMetadataChecksum,
                templatesMetadataChecksum,
                customMetadataMapChecksum,
                hashesOfConsistentSettingsChecksum,
                indicesChecksum,
                clusterStateChecksum
            );
        }
    }

}
