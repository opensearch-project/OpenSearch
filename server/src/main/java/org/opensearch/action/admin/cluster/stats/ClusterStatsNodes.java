/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest.Metric;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.DiscoveryModule;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per Node Cluster Stats
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ClusterStatsNodes implements ToXContentFragment {

    private final Counts counts;
    private final Set<Version> versions;
    private final OsStats os;
    private final ProcessStats process;
    private final JvmStats jvm;
    private final FsInfo.Path fs;
    private final Set<PluginInfo> plugins;
    private final NetworkTypes networkTypes;
    private final DiscoveryTypes discoveryTypes;
    private final PackagingTypes packagingTypes;
    private final IngestStats ingestStats;

    public static final Set<Metric> NODE_STATS_METRICS = Set.of(
        // Stats computed from node info and node stat
        Metric.OS,
        Metric.JVM,
        // Stats computed from node stat
        Metric.FS,
        Metric.PROCESS,
        Metric.INGEST,
        // Stats computed from node info
        Metric.PLUGINS,
        Metric.NETWORK_TYPES,
        Metric.DISCOVERY_TYPES,
        Metric.PACKAGING_TYPES
    );

    ClusterStatsNodes(List<ClusterStatsNodeResponse> nodeResponses) {
        this(Set.of(Metric.values()), nodeResponses);
    }

    ClusterStatsNodes(Set<Metric> requestedMetrics, List<ClusterStatsNodeResponse> nodeResponses) {
        this.versions = new HashSet<>();
        this.fs = requestedMetrics.contains(ClusterStatsRequest.Metric.FS) ? new FsInfo.Path() : null;
        this.plugins = requestedMetrics.contains(ClusterStatsRequest.Metric.PLUGINS) ? new HashSet<>() : null;

        Set<InetAddress> seenAddresses = new HashSet<>(nodeResponses.size());
        List<NodeInfo> nodeInfos = new ArrayList<>(nodeResponses.size());
        List<NodeStats> nodeStats = new ArrayList<>(nodeResponses.size());
        for (ClusterStatsNodeResponse nodeResponse : nodeResponses) {
            nodeInfos.add(nodeResponse.nodeInfo());
            nodeStats.add(nodeResponse.nodeStats());
            this.versions.add(nodeResponse.nodeInfo().getVersion());
            if (requestedMetrics.contains(ClusterStatsRequest.Metric.PLUGINS)) {
                this.plugins.addAll(nodeResponse.nodeInfo().getInfo(PluginsAndModules.class).getPluginInfos());
            }

            // now do the stats that should be deduped by hardware (implemented by ip deduping)
            TransportAddress publishAddress = nodeResponse.nodeInfo().getInfo(TransportInfo.class).address().publishAddress();
            final InetAddress inetAddress = publishAddress.address().getAddress();
            if (!seenAddresses.add(inetAddress)) {
                continue;
            }
            if (requestedMetrics.contains(ClusterStatsRequest.Metric.FS) && nodeResponse.nodeStats().getFs() != null) {
                this.fs.add(nodeResponse.nodeStats().getFs().getTotal());
            }
        }

        this.counts = new Counts(nodeInfos);
        this.networkTypes = requestedMetrics.contains(ClusterStatsRequest.Metric.NETWORK_TYPES) ? new NetworkTypes(nodeInfos) : null;
        this.discoveryTypes = requestedMetrics.contains(ClusterStatsRequest.Metric.DISCOVERY_TYPES) ? new DiscoveryTypes(nodeInfos) : null;
        this.packagingTypes = requestedMetrics.contains(ClusterStatsRequest.Metric.PACKAGING_TYPES) ? new PackagingTypes(nodeInfos) : null;
        this.ingestStats = requestedMetrics.contains(ClusterStatsRequest.Metric.INGEST) ? new IngestStats(nodeStats) : null;
        this.process = requestedMetrics.contains(ClusterStatsRequest.Metric.PROCESS) ? new ProcessStats(nodeStats) : null;
        this.os = requestedMetrics.contains(ClusterStatsRequest.Metric.OS) ? new OsStats(nodeInfos, nodeStats) : null;
        this.jvm = requestedMetrics.contains(ClusterStatsRequest.Metric.JVM) ? new JvmStats(nodeInfos, nodeStats) : null;
    }

    public Counts getCounts() {
        return this.counts;
    }

    public Set<Version> getVersions() {
        return versions;
    }

    public OsStats getOs() {
        return os;
    }

    public ProcessStats getProcess() {
        return process;
    }

    public JvmStats getJvm() {
        return jvm;
    }

    public FsInfo.Path getFs() {
        return fs;
    }

    public Set<PluginInfo> getPlugins() {
        return plugins;
    }

    /**
     * Inner Fields used for creating XContent and parsing
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String COUNT = "count";
        static final String VERSIONS = "versions";
        static final String OS = "os";
        static final String PROCESS = "process";
        static final String JVM = "jvm";
        static final String FS = "fs";
        static final String PLUGINS = "plugins";
        static final String NETWORK_TYPES = "network_types";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.COUNT);
        counts.toXContent(builder, params);
        builder.endObject();

        builder.startArray(Fields.VERSIONS);
        for (Version v : versions) {
            builder.value(v.toString());
        }
        builder.endArray();

        if (os != null) {
            builder.startObject(Fields.OS);
            os.toXContent(builder, params);
            builder.endObject();
        }

        if (process != null) {
            builder.startObject(Fields.PROCESS);
            process.toXContent(builder, params);
            builder.endObject();
        }

        if (jvm != null) {
            builder.startObject(Fields.JVM);
            jvm.toXContent(builder, params);
            builder.endObject();
        }

        if (fs != null) {
            builder.field(Fields.FS);
            fs.toXContent(builder, params);
        }

        if (plugins != null) {
            builder.startArray(Fields.PLUGINS);
            for (PluginInfo pluginInfo : plugins) {
                pluginInfo.toXContent(builder, params);
            }
            builder.endArray();
        }

        if (networkTypes != null) {
            builder.startObject(Fields.NETWORK_TYPES);
            networkTypes.toXContent(builder, params);
            builder.endObject();
        }

        if (discoveryTypes != null) {
            discoveryTypes.toXContent(builder, params);
        }

        if (packagingTypes != null) {
            packagingTypes.toXContent(builder, params);
        }

        if (ingestStats != null) {
            ingestStats.toXContent(builder, params);
        }

        return builder;
    }

    /**
     * Inner Counts
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Counts implements ToXContentFragment {
        static final String COORDINATING_ONLY = "coordinating_only";

        private final int total;
        private final Map<String, Integer> roles;

        private Counts(final List<NodeInfo> nodeInfos) {
            // TODO: do we need to report zeros?
            final Map<String, Integer> roles = new HashMap<>(DiscoveryNode.getPossibleRoleNames().size());
            roles.put(COORDINATING_ONLY, 0);
            for (final String possibleRoleName : DiscoveryNode.getPossibleRoleNames()) {
                roles.put(possibleRoleName, 0);
            }

            int total = 0;
            for (final NodeInfo nodeInfo : nodeInfos) {
                total++;
                if (nodeInfo.getNode().getRoles().isEmpty()) {
                    roles.merge(COORDINATING_ONLY, 1, Integer::sum);
                } else {
                    for (DiscoveryNodeRole role : nodeInfo.getNode().getRoles()) {
                        // TODO: Remove the 'if' condition and only keep the statement in 'else' after removing MASTER_ROLE.
                        // As of 2.0, CLUSTER_MANAGER_ROLE is added, and it should be taken as MASTER_ROLE
                        if (role.isClusterManager()) {
                            roles.merge(DiscoveryNodeRole.MASTER_ROLE.roleName(), 1, Integer::sum);
                            roles.merge(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(), 1, Integer::sum);
                        } else {
                            roles.merge(role.roleName(), 1, Integer::sum);
                        }
                    }
                }
            }
            this.total = total;
            this.roles = Collections.unmodifiableMap(new HashMap<>(roles));
        }

        public int getTotal() {
            return total;
        }

        public Map<String, Integer> getRoles() {
            return roles;
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String TOTAL = "total";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.TOTAL, total);
            for (Map.Entry<String, Integer> entry : new TreeMap<>(roles).entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            return builder;
        }
    }

    /**
     * Inner Operating System Stats
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class OsStats implements ToXContentFragment {
        final int availableProcessors;
        final int allocatedProcessors;
        final Map<String, Integer> names;
        final Map<String, Integer> prettyNames;
        final org.opensearch.monitor.os.OsStats.Mem mem;

        /**
         * Build the stats from information about each node.
         */
        private OsStats(List<NodeInfo> nodeInfos, List<NodeStats> nodeStatsList) {
            final Map<String, Integer> names = new HashMap<>(nodeInfos.size());
            final Map<String, Integer> prettyNames = new HashMap<>(nodeInfos.size());
            int availableProcessors = 0;
            int allocatedProcessors = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                availableProcessors += nodeInfo.getInfo(OsInfo.class).getAvailableProcessors();
                allocatedProcessors += nodeInfo.getInfo(OsInfo.class).getAllocatedProcessors();

                if (nodeInfo.getInfo(OsInfo.class).getName() != null) {
                    names.merge(nodeInfo.getInfo(OsInfo.class).getName(), 1, Integer::sum);
                }
                if (nodeInfo.getInfo(OsInfo.class).getPrettyName() != null) {
                    prettyNames.merge(nodeInfo.getInfo(OsInfo.class).getPrettyName(), 1, Integer::sum);
                }
            }
            this.names = Collections.unmodifiableMap(names);
            this.prettyNames = Collections.unmodifiableMap(prettyNames);
            this.availableProcessors = availableProcessors;
            this.allocatedProcessors = allocatedProcessors;

            long totalMemory = 0;
            long freeMemory = 0;
            for (NodeStats nodeStats : nodeStatsList) {
                if (nodeStats.getOs() != null) {
                    long total = nodeStats.getOs().getMem().getTotal().getBytes();
                    if (total > 0) {
                        totalMemory += total;
                    }
                    long free = nodeStats.getOs().getMem().getFree().getBytes();
                    if (free > 0) {
                        freeMemory += free;
                    }
                }
            }
            this.mem = new org.opensearch.monitor.os.OsStats.Mem(totalMemory, freeMemory);
        }

        public int getAvailableProcessors() {
            return availableProcessors;
        }

        public int getAllocatedProcessors() {
            return allocatedProcessors;
        }

        public org.opensearch.monitor.os.OsStats.Mem getMem() {
            return mem;
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String AVAILABLE_PROCESSORS = "available_processors";
            static final String ALLOCATED_PROCESSORS = "allocated_processors";
            static final String NAME = "name";
            static final String NAMES = "names";
            static final String PRETTY_NAME = "pretty_name";
            static final String PRETTY_NAMES = "pretty_names";
            static final String COUNT = "count";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.AVAILABLE_PROCESSORS, availableProcessors);
            builder.field(Fields.ALLOCATED_PROCESSORS, allocatedProcessors);
            builder.startArray(Fields.NAMES);
            {
                for (final Map.Entry<String, Integer> name : names.entrySet()) {
                    builder.startObject();
                    {
                        builder.field(Fields.NAME, name.getKey());
                        builder.field(Fields.COUNT, name.getValue());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            builder.startArray(Fields.PRETTY_NAMES);
            {
                for (final Map.Entry<String, Integer> prettyName : prettyNames.entrySet()) {
                    builder.startObject();
                    {
                        builder.field(Fields.PRETTY_NAME, prettyName.getKey());
                        builder.field(Fields.COUNT, prettyName.getValue());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            mem.toXContent(builder, params);
            return builder;
        }
    }

    /**
     * Inner Process Stats
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ProcessStats implements ToXContentFragment {

        final int count;
        final int cpuPercent;
        final long totalOpenFileDescriptors;
        final long minOpenFileDescriptors;
        final long maxOpenFileDescriptors;

        /**
         * Build from looking at a list of node statistics.
         */
        private ProcessStats(List<NodeStats> nodeStatsList) {
            int count = 0;
            int cpuPercent = 0;
            long totalOpenFileDescriptors = 0;
            long minOpenFileDescriptors = Long.MAX_VALUE;
            long maxOpenFileDescriptors = Long.MIN_VALUE;
            for (NodeStats nodeStats : nodeStatsList) {
                if (nodeStats.getProcess() == null) {
                    continue;
                }
                count++;
                if (nodeStats.getProcess().getCpu() != null) {
                    cpuPercent += nodeStats.getProcess().getCpu().getPercent();
                }
                long fd = nodeStats.getProcess().getOpenFileDescriptors();
                if (fd > 0) {
                    // fd can be -1 if not supported on platform
                    totalOpenFileDescriptors += fd;
                }
                // we still do min max calc on -1, so we'll have an indication
                // of it not being supported on one of the nodes.
                minOpenFileDescriptors = Math.min(minOpenFileDescriptors, fd);
                maxOpenFileDescriptors = Math.max(maxOpenFileDescriptors, fd);
            }
            this.count = count;
            this.cpuPercent = cpuPercent;
            this.totalOpenFileDescriptors = totalOpenFileDescriptors;
            this.minOpenFileDescriptors = minOpenFileDescriptors;
            this.maxOpenFileDescriptors = maxOpenFileDescriptors;
        }

        /**
         * Cpu usage in percentages - 100 is 1 core.
         */
        public int getCpuPercent() {
            return cpuPercent;
        }

        public long getAvgOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return totalOpenFileDescriptors / count;
        }

        public long getMaxOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return maxOpenFileDescriptors;
        }

        public long getMinOpenFileDescriptors() {
            if (count == 0) {
                return -1;
            }
            return minOpenFileDescriptors;
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String CPU = "cpu";
            static final String PERCENT = "percent";
            static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
            static final String MIN = "min";
            static final String MAX = "max";
            static final String AVG = "avg";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(Fields.CPU).field(Fields.PERCENT, cpuPercent).endObject();
            if (count > 0) {
                builder.startObject(Fields.OPEN_FILE_DESCRIPTORS);
                builder.field(Fields.MIN, getMinOpenFileDescriptors());
                builder.field(Fields.MAX, getMaxOpenFileDescriptors());
                builder.field(Fields.AVG, getAvgOpenFileDescriptors());
                builder.endObject();
            }
            return builder;
        }
    }

    /**
     * Inner JVM Stats
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class JvmStats implements ToXContentFragment {

        private final Map<JvmVersion, Integer> versions;
        private final long threads;
        private final long maxUptime;
        private final long heapUsed;
        private final long heapMax;

        /**
         * Build from lists of information about each node.
         */
        private JvmStats(List<NodeInfo> nodeInfos, List<NodeStats> nodeStatsList) {
            final Map<JvmVersion, Integer> versions = new HashMap<>(nodeInfos.size());
            long threads = 0;
            long maxUptime = 0;
            long heapMax = 0;
            long heapUsed = 0;
            for (NodeInfo nodeInfo : nodeInfos) {
                versions.merge(new JvmVersion(nodeInfo.getInfo(JvmInfo.class)), 1, Integer::sum);
            }
            this.versions = Collections.unmodifiableMap(versions);
            for (NodeStats nodeStats : nodeStatsList) {
                org.opensearch.monitor.jvm.JvmStats js = nodeStats.getJvm();
                if (js == null) {
                    continue;
                }
                if (js.getThreads() != null) {
                    threads += js.getThreads().getCount();
                }
                maxUptime = Math.max(maxUptime, js.getUptime().millis());
                if (js.getMem() != null) {
                    heapUsed += js.getMem().getHeapUsed().getBytes();
                    heapMax += js.getMem().getHeapMax().getBytes();
                }
            }
            this.threads = threads;
            this.maxUptime = maxUptime;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
        }

        public Map<JvmVersion, Integer> getVersions() {
            return versions;
        }

        /**
         * The total number of threads in the cluster
         */
        public long getThreads() {
            return threads;
        }

        /**
         * The maximum uptime of a node in the cluster
         */
        public TimeValue getMaxUpTime() {
            return new TimeValue(maxUptime);
        }

        /**
         * Total heap used in the cluster
         */
        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * Maximum total heap available to the cluster
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        /**
         * Inner Fields used for creating XContent and parsing
         *
         * @opensearch.internal
         */
        static final class Fields {
            static final String VERSIONS = "versions";
            static final String VERSION = "version";
            static final String VM_NAME = "vm_name";
            static final String VM_VERSION = "vm_version";
            static final String VM_VENDOR = "vm_vendor";
            static final String BUNDLED_JDK = "bundled_jdk";
            static final String USING_BUNDLED_JDK = "using_bundled_jdk";
            static final String COUNT = "count";
            static final String THREADS = "threads";
            static final String MAX_UPTIME = "max_uptime";
            static final String MAX_UPTIME_IN_MILLIS = "max_uptime_in_millis";
            static final String MEM = "mem";
            static final String HEAP_USED = "heap_used";
            static final String HEAP_USED_IN_BYTES = "heap_used_in_bytes";
            static final String HEAP_MAX = "heap_max";
            static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.MAX_UPTIME_IN_MILLIS, Fields.MAX_UPTIME, new TimeValue(maxUptime));
            builder.startArray(Fields.VERSIONS);
            for (final Map.Entry<JvmVersion, Integer> v : versions.entrySet()) {
                builder.startObject();
                builder.field(Fields.VERSION, v.getKey().version);
                builder.field(Fields.VM_NAME, v.getKey().vmName);
                builder.field(Fields.VM_VERSION, v.getKey().vmVersion);
                builder.field(Fields.VM_VENDOR, v.getKey().vmVendor);
                builder.field(Fields.BUNDLED_JDK, v.getKey().bundledJdk);
                builder.field(Fields.USING_BUNDLED_JDK, v.getKey().usingBundledJdk);
                builder.field(Fields.COUNT, v.getValue());
                builder.endObject();
            }
            builder.endArray();
            builder.startObject(Fields.MEM);
            builder.humanReadableField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, getHeapUsed());
            builder.humanReadableField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, getHeapMax());
            builder.endObject();

            builder.field(Fields.THREADS, threads);
            return builder;
        }
    }

    /**
     * Inner JVM Version
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class JvmVersion {
        String version;
        String vmName;
        String vmVersion;
        String vmVendor;
        boolean bundledJdk;
        Boolean usingBundledJdk;

        JvmVersion(JvmInfo jvmInfo) {
            version = jvmInfo.version();
            vmName = jvmInfo.getVmName();
            vmVersion = jvmInfo.getVmVersion();
            vmVendor = jvmInfo.getVmVendor();
            bundledJdk = jvmInfo.getBundledJdk();
            usingBundledJdk = jvmInfo.getUsingBundledJdk();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            JvmVersion jvm = (JvmVersion) o;

            return vmVersion.equals(jvm.vmVersion) && vmVendor.equals(jvm.vmVendor);
        }

        @Override
        public int hashCode() {
            return vmVersion.hashCode();
        }
    }

    /**
     * Inner Network Types
     *
     * @opensearch.internal
     */
    static class NetworkTypes implements ToXContentFragment {

        private final Map<String, AtomicInteger> transportTypes;
        private final Map<String, AtomicInteger> httpTypes;

        NetworkTypes(final List<NodeInfo> nodeInfos) {
            final Map<String, AtomicInteger> transportTypes = new HashMap<>();
            final Map<String, AtomicInteger> httpTypes = new HashMap<>();
            for (final NodeInfo nodeInfo : nodeInfos) {
                final Settings settings = nodeInfo.getSettings();
                final String transportType = settings.get(
                    NetworkModule.TRANSPORT_TYPE_KEY,
                    NetworkModule.TRANSPORT_DEFAULT_TYPE_SETTING.get(settings)
                );
                final String httpType = settings.get(NetworkModule.HTTP_TYPE_KEY, NetworkModule.HTTP_DEFAULT_TYPE_SETTING.get(settings));
                if (Strings.hasText(transportType)) {
                    transportTypes.computeIfAbsent(transportType, k -> new AtomicInteger()).incrementAndGet();
                }
                if (Strings.hasText(httpType)) {
                    httpTypes.computeIfAbsent(httpType, k -> new AtomicInteger()).incrementAndGet();
                }
            }
            this.transportTypes = Collections.unmodifiableMap(transportTypes);
            this.httpTypes = Collections.unmodifiableMap(httpTypes);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("transport_types");
            for (final Map.Entry<String, AtomicInteger> entry : transportTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            builder.startObject("http_types");
            for (final Map.Entry<String, AtomicInteger> entry : httpTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            return builder;
        }

    }

    /**
     * Inner Discovery Types
     *
     * @opensearch.internal
     */
    static class DiscoveryTypes implements ToXContentFragment {

        private final Map<String, AtomicInteger> discoveryTypes;

        DiscoveryTypes(final List<NodeInfo> nodeInfos) {
            final Map<String, AtomicInteger> discoveryTypes = new HashMap<>();
            for (final NodeInfo nodeInfo : nodeInfos) {
                final Settings settings = nodeInfo.getSettings();
                final String discoveryType = DiscoveryModule.DISCOVERY_TYPE_SETTING.get(settings);
                discoveryTypes.computeIfAbsent(discoveryType, k -> new AtomicInteger()).incrementAndGet();
            }
            this.discoveryTypes = Collections.unmodifiableMap(discoveryTypes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject("discovery_types");
            for (final Map.Entry<String, AtomicInteger> entry : discoveryTypes.entrySet()) {
                builder.field(entry.getKey(), entry.getValue().get());
            }
            builder.endObject();
            return builder;
        }
    }

    /**
     * Inner Packaging Types
     *
     * @opensearch.internal
     */
    static class PackagingTypes implements ToXContentFragment {

        private final Map<String, AtomicInteger> packagingTypes;

        PackagingTypes(final List<NodeInfo> nodeInfos) {
            final Map<String, AtomicInteger> packagingTypes = new HashMap<>();
            for (final NodeInfo nodeInfo : nodeInfos) {
                final String type = nodeInfo.getBuild().type().displayName();
                packagingTypes.computeIfAbsent(type, k -> new AtomicInteger()).incrementAndGet();
            }
            this.packagingTypes = Collections.unmodifiableMap(packagingTypes);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startArray("packaging_types");
            {
                for (final Map.Entry<String, AtomicInteger> entry : packagingTypes.entrySet()) {
                    builder.startObject();
                    {
                        builder.field("type", entry.getKey());
                        builder.field("count", entry.getValue().get());
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
            return builder;
        }

    }

    /**
     * Inner Ingest Stats
     *
     * @opensearch.internal
     */
    static class IngestStats implements ToXContentFragment {

        final int pipelineCount;
        final SortedMap<String, long[]> stats;

        IngestStats(final List<NodeStats> nodeStats) {
            Set<String> pipelineIds = new HashSet<>();
            SortedMap<String, long[]> stats = new TreeMap<>();
            for (NodeStats nodeStat : nodeStats) {
                if (nodeStat.getIngestStats() != null) {
                    for (Map.Entry<String, List<org.opensearch.ingest.IngestStats.ProcessorStat>> processorStats : nodeStat.getIngestStats()
                        .getProcessorStats()
                        .entrySet()) {
                        pipelineIds.add(processorStats.getKey());
                        for (org.opensearch.ingest.IngestStats.ProcessorStat stat : processorStats.getValue()) {
                            stats.compute(stat.getType(), (k, v) -> {
                                OperationStats nodeIngestStats = stat.getStats();
                                if (v == null) {
                                    return new long[] {
                                        nodeIngestStats.getCount(),
                                        nodeIngestStats.getFailedCount(),
                                        nodeIngestStats.getCurrent(),
                                        nodeIngestStats.getTotalTimeInMillis() };
                                } else {
                                    v[0] += nodeIngestStats.getCount();
                                    v[1] += nodeIngestStats.getFailedCount();
                                    v[2] += nodeIngestStats.getCurrent();
                                    v[3] += nodeIngestStats.getTotalTimeInMillis();
                                    return v;
                                }
                            });
                        }
                    }
                }
            }
            this.pipelineCount = pipelineIds.size();
            this.stats = Collections.unmodifiableSortedMap(stats);
        }

        @Override
        public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
            builder.startObject("ingest");
            {
                builder.field("number_of_pipelines", pipelineCount);
                builder.startObject("processor_stats");
                for (Map.Entry<String, long[]> stat : stats.entrySet()) {
                    long[] statValues = stat.getValue();
                    builder.startObject(stat.getKey());
                    builder.field("count", statValues[0]);
                    builder.field("failed", statValues[1]);
                    builder.field("current", statValues[2]);
                    builder.humanReadableField("time_in_millis", "time", new TimeValue(statValues[3], TimeUnit.MILLISECONDS));
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

    }

}
