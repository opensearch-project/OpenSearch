/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.support.nodes.ProtobufBaseNodeResponse;
import org.opensearch.cluster.node.ProtobufDiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.http.ProtobufHttpInfo;
import org.opensearch.ingest.ProtobufIngestInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.ProtobufJvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.os.ProtobufOsInfo;
import org.opensearch.monitor.process.ProtobufProcessInfo;
import org.opensearch.node.ProtobufReportingService;
import org.opensearch.search.aggregations.support.ProtobufAggregationInfo;
import org.opensearch.search.pipeline.ProtobufSearchPipelineInfo;
import org.opensearch.threadpool.ProtobufThreadPoolInfo;
import org.opensearch.transport.ProtobufTransportInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

    /**
     * Do not expose this map to other classes. For type safety, use {@link #getInfo(Class)}
    * to retrieve items from this map and {@link #addInfoIfNonNull(Class, ProtobufReportingService.ProtobufInfo)}
    * to retrieve items from it.
    */
    private Map<Class<? extends ProtobufReportingService.ProtobufInfo>, ProtobufReportingService.ProtobufInfo> infoMap = new HashMap<>();

    @Nullable
    private ByteSizeValue totalIndexingBuffer;

    public ProtobufNodeInfo(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        version = Version.readVersionProtobuf(in);
        build = Build.readBuildProtobuf(in);
        if (in.readBool()) {
            totalIndexingBuffer = new ByteSizeValue(in.readInt64());
        } else {
            totalIndexingBuffer = null;
        }
        if (in.readBool()) {
            settings = Settings.readSettingsFromStreamProtobuf(in);
        }
        addInfoIfNonNull(ProtobufOsInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufOsInfo::new));
        addInfoIfNonNull(ProtobufProcessInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufProcessInfo::new));
        addInfoIfNonNull(ProtobufJvmInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufJvmInfo::new));
        addInfoIfNonNull(ProtobufThreadPoolInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufThreadPoolInfo::new));
        addInfoIfNonNull(ProtobufTransportInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufTransportInfo::new));
        addInfoIfNonNull(ProtobufHttpInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufHttpInfo::new));
        addInfoIfNonNull(ProtobufPluginsAndModules.class, protobufStreamInput.readOptionalWriteable(ProtobufPluginsAndModules::new));
        addInfoIfNonNull(ProtobufIngestInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufIngestInfo::new));
        addInfoIfNonNull(ProtobufAggregationInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufAggregationInfo::new));
        if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_7_0)) {
            addInfoIfNonNull(ProtobufSearchPipelineInfo.class, protobufStreamInput.readOptionalWriteable(ProtobufSearchPipelineInfo::new));
        }
    }

    public ProtobufNodeInfo(
        Version version,
        Build build,
        ProtobufDiscoveryNode node,
        @Nullable Settings settings,
        @Nullable ProtobufOsInfo os,
        @Nullable ProtobufProcessInfo process,
        @Nullable ProtobufJvmInfo jvm,
        @Nullable ProtobufThreadPoolInfo threadPool,
        @Nullable ProtobufTransportInfo transport,
        @Nullable ProtobufHttpInfo http,
        @Nullable ProtobufPluginsAndModules plugins,
        @Nullable ProtobufIngestInfo ingest,
        @Nullable ProtobufAggregationInfo aggsInfo,
        @Nullable ByteSizeValue totalIndexingBuffer,
        @Nullable ProtobufSearchPipelineInfo ProtobufSearchPipelineInfo
    ) {
        super(node);
        this.version = version;
        this.build = build;
        this.settings = settings;
        addInfoIfNonNull(ProtobufOsInfo.class, os);
        addInfoIfNonNull(ProtobufProcessInfo.class, process);
        addInfoIfNonNull(ProtobufJvmInfo.class, jvm);
        addInfoIfNonNull(ProtobufThreadPoolInfo.class, threadPool);
        addInfoIfNonNull(ProtobufTransportInfo.class, transport);
        addInfoIfNonNull(ProtobufHttpInfo.class, http);
        addInfoIfNonNull(ProtobufPluginsAndModules.class, plugins);
        addInfoIfNonNull(ProtobufIngestInfo.class, ingest);
        addInfoIfNonNull(ProtobufAggregationInfo.class, aggsInfo);
        addInfoIfNonNull(ProtobufSearchPipelineInfo.class, ProtobufSearchPipelineInfo);
        this.totalIndexingBuffer = totalIndexingBuffer;
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

    /**
     * Get a particular info object, e.g. {@link JvmInfo} or {@link OsInfo}. This
    * generic method handles all casting in order to spare client classes the
    * work of explicit casts. This {@link NodeInfo} class guarantees type
    * safety for these stored info blocks.
    *
    * @param clazz Class for retrieval.
    * @param <T>   Specific subtype of ReportingService.ProtobufInfo to retrieve.
    * @return      An object of type T.
    */
    public <T extends ProtobufReportingService.ProtobufInfo> T getInfo(Class<T> clazz) {
        return clazz.cast(infoMap.get(clazz));
    }

    @Nullable
    public ByteSizeValue getTotalIndexingBuffer() {
        return totalIndexingBuffer;
    }

    /**
     * Add a value to the map of information blocks. This method guarantees the
    * type safety of the storage of heterogeneous types of reporting service information.
    */
    private <T extends ProtobufReportingService.ProtobufInfo> void addInfoIfNonNull(Class<T> clazz, T info) {
        if (info != null) {
            infoMap.put(clazz, info);
        }
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt32NoTag(version.id);
        Build.writeBuildProtobuf(build, out);
        if (totalIndexingBuffer == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            out.writeInt64NoTag(totalIndexingBuffer.getBytes());
        }
        if (settings == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            Settings.writeSettingsToStreamProtobuf(settings, out);
        }
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufOsInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufProcessInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufJvmInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufThreadPoolInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufTransportInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufHttpInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufPluginsAndModules.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufIngestInfo.class));
        protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufAggregationInfo.class));
        if (protobufStreamOutput.getVersion().onOrAfter(Version.V_2_7_0)) {
            protobufStreamOutput.writeOptionalWriteable(getInfo(ProtobufSearchPipelineInfo.class));
        }
    }

    public static ProtobufNodeInfo.Builder builder(Version version, Build build, ProtobufDiscoveryNode node) {
        return new Builder(version, build, node);
    }

    /**
     * Builder class to accommodate new Info types being added to NodeInfo.
    */
    public static class Builder {
        private final Version version;
        private final Build build;
        private final ProtobufDiscoveryNode node;

        private Builder(Version version, Build build, ProtobufDiscoveryNode node) {
            this.version = version;
            this.build = build;
            this.node = node;
        }

        private Settings settings;
        private ProtobufOsInfo os;
        private ProtobufProcessInfo process;
        private ProtobufJvmInfo jvm;
        private ProtobufThreadPoolInfo threadPool;
        private ProtobufTransportInfo transport;
        private ProtobufHttpInfo http;
        private ProtobufPluginsAndModules plugins;
        private ProtobufIngestInfo ingest;
        private ProtobufAggregationInfo aggsInfo;
        private ByteSizeValue totalIndexingBuffer;
        private ProtobufSearchPipelineInfo ProtobufSearchPipelineInfo;

        public Builder setSettings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder setOs(ProtobufOsInfo os) {
            this.os = os;
            return this;
        }

        public Builder setProcess(ProtobufProcessInfo process) {
            this.process = process;
            return this;
        }

        public Builder setJvm(ProtobufJvmInfo jvm) {
            this.jvm = jvm;
            return this;
        }

        public Builder setThreadPool(ProtobufThreadPoolInfo threadPool) {
            this.threadPool = threadPool;
            return this;
        }

        public Builder setTransport(ProtobufTransportInfo transport) {
            this.transport = transport;
            return this;
        }

        public Builder setHttp(ProtobufHttpInfo http) {
            this.http = http;
            return this;
        }

        public Builder setPlugins(ProtobufPluginsAndModules plugins) {
            this.plugins = plugins;
            return this;
        }

        public Builder setIngest(ProtobufIngestInfo ingest) {
            this.ingest = ingest;
            return this;
        }

        public Builder setAggsInfo(ProtobufAggregationInfo aggsInfo) {
            this.aggsInfo = aggsInfo;
            return this;
        }

        public Builder setTotalIndexingBuffer(ByteSizeValue totalIndexingBuffer) {
            this.totalIndexingBuffer = totalIndexingBuffer;
            return this;
        }

        public Builder setProtobufSearchPipelineInfo(ProtobufSearchPipelineInfo ProtobufSearchPipelineInfo) {
            this.ProtobufSearchPipelineInfo = ProtobufSearchPipelineInfo;
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
                ProtobufSearchPipelineInfo
            );
        }

    }

}
