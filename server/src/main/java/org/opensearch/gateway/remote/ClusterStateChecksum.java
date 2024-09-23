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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.jcraft.jzlib.JZlib;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Stores checksum for all components in cluster state. This will be used to ensure cluster state is same across all nodes in the cluster.
 */
public class ClusterStateChecksum implements ToXContentFragment, Writeable {

    static final String ROUTING_TABLE_CS = "routing_table";
    static final String NODES_CS = "discovery_nodes";
    static final String BLOCKS_CS = "blocks";
    static final String CUSTOMS_CS = "customs";
    static final String COORDINATION_MD_CS = "coordination_md";
    static final String SETTINGS_MD_CS = "settings_md";
    static final String TRANSIENT_SETTINGS_MD_CS = "transient_settings_md";
    static final String TEMPLATES_MD_CS = "templates_md";
    static final String CUSTOM_MD_CS = "customs_md";
    static final String HASHES_MD_CS = "hashes_md";
    static final String INDICES_CS = "indices_md";
    private static final String CLUSTER_STATE_CS = "cluster_state";
    private static final int CHECKSUM_SIZE = 8;
    private static final int COMPONENT_SIZE = 11;
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
        long start = System.currentTimeMillis();
        // keeping thread pool size to number of components.
        ExecutorService executorService = Executors.newFixedThreadPool(COMPONENT_SIZE);
        CountDownLatch latch = new CountDownLatch(COMPONENT_SIZE);

        executeChecksumTask((stream) -> {
            try {
                clusterState.routingTable().writeVerifiableTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for routing table", e);
            }
            return null;
        }, checksum -> routingTableChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                clusterState.nodes().writeVerifiableTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for discovery nodes", e);
            }
            return null;
        }, checksum -> nodesChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                clusterState.coordinationMetadata().writeVerifiableTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for coordination metadata", e);
            }
            return null;
        }, checksum -> coordinationMetadataChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                Settings.writeSettingsToStream(clusterState.metadata().persistentSettings(), stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for settings metadata", e);
            }
            return null;
        }, checksum -> settingMetadataChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                Settings.writeSettingsToStream(clusterState.metadata().transientSettings(), stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for transient settings metadata", e);
            }
            return null;
        }, checksum -> transientSettingsMetadataChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                clusterState.metadata().templatesMetadata().writeVerifiableTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for templates metadata", e);
            }
            return null;
        }, checksum -> templatesMetadataChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                stream.writeStringCollection(clusterState.metadata().customs().keySet());
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for customs metadata", e);
            }
            return null;
        }, checksum -> customMetadataMapChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                ((DiffableStringMap) clusterState.metadata().hashesOfConsistentSettings()).writeTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for hashesOfConsistentSettings", e);
            }
            return null;
        }, checksum -> hashesOfConsistentSettingsChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                stream.writeMapValues(
                    clusterState.metadata().indices(),
                    (checksumStream, value) -> value.writeVerifiableTo((BufferedChecksumStreamOutput) checksumStream)
                );
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for indices metadata", e);
            }
            return null;
        }, checksum -> indicesChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                clusterState.blocks().writeVerifiableTo(stream);
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for cluster state blocks", e);
            }
            return null;
        }, checksum -> blocksChecksum = checksum, executorService, latch);

        executeChecksumTask((stream) -> {
            try {
                stream.writeStringCollection(clusterState.customs().keySet());
            } catch (IOException e) {
                throw new RemoteStateTransferException("Failed to create checksum for cluster state customs", e);
            }
            return null;
        }, checksum -> clusterStateCustomsChecksum = checksum, executorService, latch);

        executorService.shutdown();
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RemoteStateTransferException("Failed to create checksum for cluster state.", e);
        }
        createClusterStateChecksum();
        logger.debug("Checksum execution time {}", System.currentTimeMillis() - start);
    }

    private void executeChecksumTask(Function<BufferedChecksumStreamOutput, Void> checksumTask, Consumer<Long> checksumConsumer, ExecutorService executorService, CountDownLatch latch) {
        executorService.execute(() -> {
            try {
                long checksum = createChecksum(checksumTask);
                checksumConsumer.accept(checksum);
                latch.countDown();
            } catch (IOException  e) {
                throw new RemoteStateTransferException("Failed to execute checksum task", e);
            }
        });
    }

    private long createChecksum(Function<BufferedChecksumStreamOutput, Void> o) throws IOException {
        try (
            BytesStreamOutput out = new BytesStreamOutput();
            BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out)
        ) {
            o.apply(checksumOut);
            return checksumOut.getChecksum();
        }
    }

    private void createClusterStateChecksum() {
        clusterStateChecksum = JZlib.crc32_combine(routingTableChecksum, nodesChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, blocksChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, clusterStateCustomsChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, coordinationMetadataChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, settingMetadataChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, transientSettingsMetadataChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, templatesMetadataChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, customMetadataMapChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, hashesOfConsistentSettingsChecksum, CHECKSUM_SIZE);
        clusterStateChecksum = JZlib.crc32_combine(clusterStateChecksum, indicesChecksum, CHECKSUM_SIZE);
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

    @Override
    public String toString() {
        return "ClusterStateChecksum{"
            + "routingTableChecksum="
            + routingTableChecksum
            + ", nodesChecksum="
            + nodesChecksum
            + ", blocksChecksum="
            + blocksChecksum
            + ", clusterStateCustomsChecksum="
            + clusterStateCustomsChecksum
            + ", coordinationMetadataChecksum="
            + coordinationMetadataChecksum
            + ", settingMetadataChecksum="
            + settingMetadataChecksum
            + ", transientSettingsMetadataChecksum="
            + transientSettingsMetadataChecksum
            + ", templatesMetadataChecksum="
            + templatesMetadataChecksum
            + ", customMetadataMapChecksum="
            + customMetadataMapChecksum
            + ", hashesOfConsistentSettingsChecksum="
            + hashesOfConsistentSettingsChecksum
            + ", indicesChecksum="
            + indicesChecksum
            + ", clusterStateChecksum="
            + clusterStateChecksum
            + '}';
    }

    public List<String> getMismatchEntities(ClusterStateChecksum otherClusterStateChecksum) {
        if (this.clusterStateChecksum == otherClusterStateChecksum.clusterStateChecksum) {
            logger.debug("No mismatch in checksums.");
            return List.of();
        }
        List<String> mismatches = new ArrayList<>();
        addIfMismatch(this.routingTableChecksum, otherClusterStateChecksum.routingTableChecksum, ROUTING_TABLE_CS, mismatches);
        addIfMismatch(this.nodesChecksum, otherClusterStateChecksum.nodesChecksum, NODES_CS, mismatches);
        addIfMismatch(this.blocksChecksum, otherClusterStateChecksum.blocksChecksum, BLOCKS_CS, mismatches);
        addIfMismatch(this.clusterStateCustomsChecksum, otherClusterStateChecksum.clusterStateCustomsChecksum, CUSTOMS_CS, mismatches);
        addIfMismatch(
            this.coordinationMetadataChecksum,
            otherClusterStateChecksum.coordinationMetadataChecksum,
            COORDINATION_MD_CS,
            mismatches
        );
        addIfMismatch(this.settingMetadataChecksum, otherClusterStateChecksum.settingMetadataChecksum, SETTINGS_MD_CS, mismatches);
        addIfMismatch(
            this.transientSettingsMetadataChecksum,
            otherClusterStateChecksum.transientSettingsMetadataChecksum,
            TRANSIENT_SETTINGS_MD_CS,
            mismatches
        );
        addIfMismatch(this.templatesMetadataChecksum, otherClusterStateChecksum.templatesMetadataChecksum, TEMPLATES_MD_CS, mismatches);
        addIfMismatch(this.customMetadataMapChecksum, otherClusterStateChecksum.customMetadataMapChecksum, CUSTOM_MD_CS, mismatches);
        addIfMismatch(
            this.hashesOfConsistentSettingsChecksum,
            otherClusterStateChecksum.hashesOfConsistentSettingsChecksum,
            HASHES_MD_CS,
            mismatches
        );
        addIfMismatch(this.indicesChecksum, otherClusterStateChecksum.indicesChecksum, INDICES_CS, mismatches);

        return mismatches;
    }

    private void addIfMismatch(long checksum, long otherChecksum, String entityName, List<String> mismatches) {
        if (checksum != otherChecksum) {
            mismatches.add(entityName);
        }
    }

    /**
     * Builder for ClusterStateChecksum
     */
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
