/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.InvalidProtocolBufferException;

import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.ProtobufBaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.http.HttpInfo;
import org.opensearch.ingest.IngestInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.search.aggregations.support.AggregationInfo;
import org.opensearch.search.pipeline.SearchPipelineInfo;
import org.opensearch.server.proto.NodesInfoProto;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Node information (static, does not change over time).
*
* @opensearch.internal
*/
public class ProtobufNodeInfo extends ProtobufBaseNodeResponse {

    private Version version;
    private Build build;

    @Nullable
    private Settings settings;

    private NodesInfoProto.NodesInfo nodesInfoResponse;

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    public ProtobufNodeInfo(byte[] data) throws InvalidProtocolBufferException {
        super(data);
        this.nodesInfoResponse = NodesInfoProto.NodesInfo.parseFrom(data);
    }

    public ProtobufNodeInfo(NodesInfoProto.NodesInfo nodesInfo) throws InvalidProtocolBufferException {
        super(nodesInfo.toByteArray());
        this.nodesInfoResponse = nodesInfo;
    }

    public ProtobufNodeInfo(
        Version version,
        Build build,
        DiscoveryNode node,
        @Nullable Settings settings,
        @Nullable OsInfo os,
        @Nullable ProcessInfo process,
        @Nullable JvmInfo jvm,
        @Nullable ThreadPoolInfo threadPool,
        @Nullable TransportInfo transport,
        @Nullable HttpInfo http,
        @Nullable PluginsAndModules plugins,
        @Nullable IngestInfo ingest,
        @Nullable AggregationInfo aggsInfo,
        @Nullable ByteSizeValue totalIndexingBuffer,
        @Nullable SearchPipelineInfo searchPipelineInfo
    ) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        this.totalIndexingBuffer = totalIndexingBuffer;
        this.nodesInfoResponse = NodesInfoProto.NodesInfo.newBuilder()
            .setNodeId(node.getId())
            .setProcessId(process.getId())
            .setAddress(http.getAddress().publishAddress().toString())
            .setDisplayName(this.build.type().displayName())
            .setHash(this.build.hash())
            .setJvmInfoVersion(jvm.version())
            .setJvmHeapMax(jvm.getMem().getHeapMax().toString())
            .build();
    }

    /**
     * System's hostname. <code>null</code> in case of UnknownHostException
    */
    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    /**
     * The current OpenSearch version
    */
    public Version getVersion() {
        return version;
    }

    /**
     * The build version of the node.
    */
    public Build getBuild() {
        return this.build;
    }

    /**
     * The settings of the node.
    */
    @Nullable
    public Settings getSettings() {
        return this.settings;
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    public static ProtobufNodeInfo.Builder builder(Version version, Build build, DiscoveryNode node) {
        return new Builder(version, build, node);
    }

    /**
     * Builder class to accommodate new Info types being added to NodeInfo.
    */
    public static class Builder {
        private final Version version;
        private final Build build;
        private final DiscoveryNode node;

        private Builder(Version version, Build build, DiscoveryNode node) {
            this.version = version;
            this.build = build;
            this.node = node;
        }

        private Settings settings;
        private OsInfo os;
        private ProcessInfo process;
        private JvmInfo jvm;
        private ThreadPoolInfo threadPool;
        private TransportInfo transport;
        private HttpInfo http;
        private PluginsAndModules plugins;
        private IngestInfo ingest;
        private AggregationInfo aggsInfo;
        private ByteSizeValue totalIndexingBuffer;
        private SearchPipelineInfo searchPipelineInfo;

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setOs(OsInfo os) {
            this.os = os;
            return this;
        }

        public Builder setProcess(ProcessInfo process) {
            this.process = process;
            return this;
        }

        public Builder setJvm(JvmInfo jvm) {
            this.jvm = jvm;
            return this;
        }

        public Builder setThreadPool(ThreadPoolInfo threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder setTransport(TransportInfo transport) {
            this.transport = transport;
            return this;
        }

        public Builder setHttp(HttpInfo http) {
            this.http = http;
            return this;
        }

        public Builder setPlugins(PluginsAndModules plugins) {
            this.plugins = plugins;
            return this;
        }

        public Builder setIngest(IngestInfo ingest) {
            this.ingest = ingest;
            return this;
        }

        public Builder setAggsInfo(AggregationInfo aggsInfo) {
            this.aggsInfo = aggsInfo;
            return this;
        }

        public Builder setTotalIndexingBuffer(ByteSizeValue totalIndexingBuffer) {
            this.totalIndexingBuffer = totalIndexingBuffer;
            return this;
        }

        public Builder setProtobufSearchPipelineInfo(SearchPipelineInfo searchPipelineInfo) {
            this.searchPipelineInfo = searchPipelineInfo;
            return this;
        }

        public ProtobufNodeInfo build() {
            return new ProtobufNodeInfo(
                version,
                build,
                node,
                settings,
                os,
                process,
                jvm,
                threadPool,
                transport,
                http,
                plugins,
                ingest,
                aggsInfo,
                totalIndexingBuffer,
                searchPipelineInfo
            );
        }

    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.nodesInfoResponse.toByteArray());
    }

    public NodesInfoProto.NodesInfo response() {
        return this.nodesInfoResponse;
    }

}
