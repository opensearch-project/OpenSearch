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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.nodesinfo;

import org.opensearch.Build;
import org.opensearch.action.admin.cluster.node.info.NodeAnalysisComponents;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.transport.BoundTransportAddress;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpInfo;
import org.opensearch.ingest.IngestInfo;
import org.opensearch.ingest.ProcessorInfo;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.monitor.process.ProcessInfo;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.search.aggregations.support.AggregationInfo;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.search.pipeline.SearchPipelineInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolInfo;
import org.opensearch.transport.TransportInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

public class NodeInfoStreamingTests extends OpenSearchTestCase {

    public void testNodeInfoStreaming() throws IOException {
        NodeInfo nodeInfo = createNodeInfo();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeInfo.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeInfo readNodeInfo = new NodeInfo(in);
                assertExpectedUnchanged(nodeInfo, readNodeInfo);
            }
        }
    }

    public void testNodeInfoPluginComponentsNaturalOrder() throws IOException {
        NodeAnalysisComponents nodeAnalysisComponents = createNodeAnalyzers();
        assertOrdered(nodeAnalysisComponents.getAnalyzersIds());
        assertOrdered(nodeAnalysisComponents.getTokenizersIds());
        assertOrdered(nodeAnalysisComponents.getTokenFiltersIds());
        assertOrdered(nodeAnalysisComponents.getCharFiltersIds());
        assertOrdered(nodeAnalysisComponents.getNormalizersIds());

        for (NodeAnalysisComponents.AnalysisPluginComponents nodeAnalysisPlugin : nodeAnalysisComponents.getNodeAnalysisPlugins()) {
            assertOrdered(nodeAnalysisPlugin.getAnalyzersIds());
            assertOrdered(nodeAnalysisPlugin.getTokenizersIds());
            assertOrdered(nodeAnalysisPlugin.getTokenFiltersIds());
            assertOrdered(nodeAnalysisPlugin.getCharFiltersIds());
            assertOrdered(nodeAnalysisPlugin.getHunspellDictionaries());
        }
    }

    private void assertOrdered(Set<String> set) {
        Iterator<String> it = set.iterator();
        if (it.hasNext()) {
            String prev = it.next();
            while (it.hasNext()) {
                String curr = it.next();
                assertTrue("Elements not naturally ordered", prev.compareTo(curr) < 0);
            }
        }
    }

    // checks all properties that are expected to be unchanged.
    // Once we start changing them between versions this method has to be changed as well
    private void assertExpectedUnchanged(NodeInfo nodeInfo, NodeInfo readNodeInfo) throws IOException {
        assertThat(nodeInfo.getBuild().toString(), equalTo(readNodeInfo.getBuild().toString()));
        assertThat(nodeInfo.getHostname(), equalTo(readNodeInfo.getHostname()));
        assertThat(nodeInfo.getVersion(), equalTo(readNodeInfo.getVersion()));
        compareJsonOutput(nodeInfo.getInfo(HttpInfo.class), readNodeInfo.getInfo(HttpInfo.class));
        compareJsonOutput(nodeInfo.getInfo(JvmInfo.class), readNodeInfo.getInfo(JvmInfo.class));
        compareJsonOutput(nodeInfo.getInfo(ProcessInfo.class), readNodeInfo.getInfo(ProcessInfo.class));
        compareJsonOutput(nodeInfo.getSettings(), readNodeInfo.getSettings());
        compareJsonOutput(nodeInfo.getInfo(ThreadPoolInfo.class), readNodeInfo.getInfo(ThreadPoolInfo.class));
        compareJsonOutput(nodeInfo.getInfo(TransportInfo.class), readNodeInfo.getInfo(TransportInfo.class));
        compareJsonOutput(nodeInfo.getNode(), readNodeInfo.getNode());
        compareJsonOutput(nodeInfo.getInfo(OsInfo.class), readNodeInfo.getInfo(OsInfo.class));
        compareJsonOutput(nodeInfo.getInfo(PluginsAndModules.class), readNodeInfo.getInfo(PluginsAndModules.class));
        compareJsonOutput(nodeInfo.getInfo(IngestInfo.class), readNodeInfo.getInfo(IngestInfo.class));
        compareJsonOutput(nodeInfo.getInfo(NodeAnalysisComponents.class), readNodeInfo.getInfo(NodeAnalysisComponents.class));
    }

    private void compareJsonOutput(ToXContent param1, ToXContent param2) throws IOException {
        if (param1 == null) {
            assertNull(param2);
            return;
        }
        ToXContent.Params params = ToXContent.EMPTY_PARAMS;
        XContentBuilder param1Builder = jsonBuilder();
        param1Builder.startObject();
        param1.toXContent(param1Builder, params);
        param1Builder.endObject();

        XContentBuilder param2Builder = jsonBuilder();
        param2Builder.startObject();
        param2.toXContent(param2Builder, params);
        param2Builder.endObject();
        assertThat(param1Builder.toString(), equalTo(param2Builder.toString()));
    }

    private static NodeInfo createNodeInfo() {
        Build build = Build.CURRENT;
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        Settings settings = randomBoolean() ? null : Settings.builder().put("test", "setting").build();
        OsInfo osInfo = null;
        if (randomBoolean()) {
            int availableProcessors = randomIntBetween(1, 64);
            int allocatedProcessors = randomIntBetween(1, availableProcessors);
            long refreshInterval = randomBoolean() ? -1 : randomNonNegativeLong();
            String name = randomAlphaOfLengthBetween(3, 10);
            String arch = randomAlphaOfLengthBetween(3, 10);
            String version = randomAlphaOfLengthBetween(3, 10);
            osInfo = new OsInfo(refreshInterval, availableProcessors, allocatedProcessors, name, name, arch, version);
        }
        ProcessInfo process = randomBoolean() ? null : new ProcessInfo(randomInt(), randomBoolean(), randomNonNegativeLong());
        JvmInfo jvm = randomBoolean() ? null : JvmInfo.jvmInfo();
        ThreadPoolInfo threadPoolInfo = null;
        if (randomBoolean()) {
            int numThreadPools = randomIntBetween(1, 10);
            List<ThreadPool.Info> threadPoolInfos = new ArrayList<>(numThreadPools);
            for (int i = 0; i < numThreadPools; i++) {
                threadPoolInfos.add(
                    new ThreadPool.Info(randomAlphaOfLengthBetween(3, 10), randomFrom(ThreadPool.ThreadPoolType.values()), randomInt())
                );
            }
            threadPoolInfo = new ThreadPoolInfo(threadPoolInfos);
        }
        Map<String, BoundTransportAddress> profileAddresses = new HashMap<>();
        BoundTransportAddress dummyBoundTransportAddress = new BoundTransportAddress(
            new TransportAddress[] { buildNewFakeTransportAddress() },
            buildNewFakeTransportAddress()
        );
        profileAddresses.put("test_address", dummyBoundTransportAddress);
        TransportInfo transport = randomBoolean() ? null : new TransportInfo(dummyBoundTransportAddress, profileAddresses);
        HttpInfo httpInfo = randomBoolean() ? null : new HttpInfo(dummyBoundTransportAddress, randomNonNegativeLong());

        PluginsAndModules pluginsAndModules = null;
        if (randomBoolean()) {
            int numPlugins = randomIntBetween(0, 5);
            List<PluginInfo> plugins = new ArrayList<>();
            for (int i = 0; i < numPlugins; i++) {
                String name = randomAlphaOfLengthBetween(3, 10);
                plugins.add(
                    new PluginInfo(
                        name,
                        randomAlphaOfLengthBetween(3, 10),
                        randomAlphaOfLengthBetween(3, 10),
                        VersionUtils.randomVersion(random()),
                        "1.8",
                        randomAlphaOfLengthBetween(3, 10),
                        name,
                        Collections.emptyList(),
                        randomBoolean()
                    )
                );
            }
            int numModules = randomIntBetween(0, 5);
            List<PluginInfo> modules = new ArrayList<>();
            for (int i = 0; i < numModules; i++) {
                String name = randomAlphaOfLengthBetween(3, 10);
                modules.add(
                    new PluginInfo(
                        name,
                        randomAlphaOfLengthBetween(3, 10),
                        randomAlphaOfLengthBetween(3, 10),
                        VersionUtils.randomVersion(random()),
                        "1.8",
                        randomAlphaOfLengthBetween(3, 10),
                        name,
                        Collections.emptyList(),
                        randomBoolean()
                    )
                );
            }
            pluginsAndModules = new PluginsAndModules(plugins, modules);
        }

        IngestInfo ingestInfo = null;
        if (randomBoolean()) {
            int numProcessors = randomIntBetween(0, 5);
            List<ProcessorInfo> processors = new ArrayList<>(numProcessors);
            for (int i = 0; i < numProcessors; i++) {
                processors.add(new ProcessorInfo(randomAlphaOfLengthBetween(3, 10)));
            }
            ingestInfo = new IngestInfo(processors);
        }

        AggregationInfo aggregationInfo = null;
        if (randomBoolean()) {
            AggregationUsageService.Builder builder = new AggregationUsageService.Builder();
            int numOfAggs = randomIntBetween(0, 10);
            for (int i = 0; i < numOfAggs; i++) {
                String aggName = randomAlphaOfLength(10);

                try {
                    if (randomBoolean()) {
                        builder.registerAggregationUsage(aggName);
                    } else {
                        int numOfTypes = randomIntBetween(1, 10);
                        for (int j = 0; j < numOfTypes; j++) {
                            builder.registerAggregationUsage(aggName, randomAlphaOfLength(10));
                        }
                    }
                } catch (IllegalArgumentException ex) {
                    // Ignore duplicate strings
                }
            }
            aggregationInfo = builder.build().info();
        }

        ByteSizeValue indexingBuffer = null;
        if (randomBoolean()) {
            // pick a random long that sometimes exceeds an int:
            indexingBuffer = new ByteSizeValue(random().nextLong() & ((1L << 40) - 1));
        }

        SearchPipelineInfo searchPipelineInfo = null;
        if (randomBoolean()) {
            int numProcessors = randomIntBetween(0, 5);
            List<org.opensearch.search.pipeline.ProcessorInfo> processors = new ArrayList<>(numProcessors);
            for (int i = 0; i < numProcessors; i++) {
                processors.add(new org.opensearch.search.pipeline.ProcessorInfo(randomAlphaOfLengthBetween(3, 10)));
            }
            searchPipelineInfo = new SearchPipelineInfo(Map.of(randomAlphaOfLengthBetween(3, 10), processors));
        }

        NodeAnalysisComponents nodeAnalysisComponents = null;
        if (randomBoolean()) {
            nodeAnalysisComponents = createNodeAnalyzers();
        }

        return new NodeInfo(
            VersionUtils.randomVersion(random()),
            build,
            node,
            settings,
            osInfo,
            process,
            jvm,
            threadPoolInfo,
            transport,
            httpInfo,
            pluginsAndModules,
            ingestInfo,
            aggregationInfo,
            indexingBuffer,
            searchPipelineInfo,
            nodeAnalysisComponents
        );
    }

    private static NodeAnalysisComponents createNodeAnalyzers() {
        List<NodeAnalysisComponents.AnalysisPluginComponents> nodeAnalysisPlugins = generateAnalysisPlugins(randomInt(5));

        return new NodeAnalysisComponents(
            generateCodes(),
            generateCodes(),
            generateCodes(),
            generateCodes(),
            generateCodes(),
            nodeAnalysisPlugins
        );
    }

    private static List<NodeAnalysisComponents.AnalysisPluginComponents> generateAnalysisPlugins(int numberOfPlugins) {
        assert numberOfPlugins > -1;
        List<NodeAnalysisComponents.AnalysisPluginComponents> plugins = new ArrayList<>();
        for (int i = 0; i < numberOfPlugins; i++) {
            NodeAnalysisComponents.AnalysisPluginComponents plugin = new NodeAnalysisComponents.AnalysisPluginComponents(
                generateRandomStringArray(1, 10, false, false)[0], // plugin name
                generateRandomStringArray(1, 10, false, false)[0], // plugin classname
                generateCodes(),
                generateCodes(),
                generateCodes(),
                generateCodes(),
                generateCodes()
            );
            plugins.add(plugin);
        }
        return plugins;
    }

    private static Set<String> generateCodes() {
        return randomUnique(CODES_SUPPLIER, NodeInfoStreamingTests.StringSetSupplier.RECOMMENDED_SIZE);
    }

    private static NodeInfoStreamingTests.StringSetSupplier CODES_SUPPLIER = new NodeInfoStreamingTests.StringSetSupplier();

    private static class StringSetSupplier implements Supplier<String> {

        private static List<String> CODES = List.of(
            "aaa1",
            "bbb1",
            "ccc1",
            "ddd1",
            "eee1",
            "fff1",
            "ggg1",
            "hhh1",
            "iii1",
            "jjj1",
            "aaa2",
            "bbb2",
            "ccc2",
            "ddd2",
            "eee2",
            "fff2",
            "ggg2",
            "hhh2",
            "iii2",
            "jjj2",
            "aaa3",
            "bbb3",
            "ccc3",
            "ddd3",
            "eee3",
            "fff3",
            "ggg3",
            "hhh3",
            "iii3",
            "jjj3"
        );

        /**
         *  This supplier is used to generate UNIQUE tokens (see {@link #generateCodes()})
         *  thus we return smaller size in order to increase the chance of yielding more unique tokens.
         *  As a result, this supplier will produce set of 10 or less unique tokens.
         */
        private static int RECOMMENDED_SIZE = CODES.size() / 3;

        @Override
        public String get() {
            return CODES.get(randomInt(CODES.size() - 1));
        }
    }
}
