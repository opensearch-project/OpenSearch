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

package org.opensearch.ingest;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.node.NodeService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 0, numClientNodes = 0, scope = OpenSearchIntegTestCase.Scope.TEST)
public class IngestProcessorNotInstalledOnAllNodesIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public IngestProcessorNotInstalledOnAllNodesIT(Settings settings) throws IOException {
        super(settings);
        pipelineSource = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("processors")
                .startObject()
                .startObject("test")
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return replicationSettings;
    }

    private final BytesReference pipelineSource;
    private volatile boolean installPlugin;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return installPlugin ? Arrays.asList(IngestTestPlugin.class) : Collections.emptyList();
    }

    public void testFailPipelineCreation() throws Exception {
        installPlugin = true;
        String node1 = internalCluster().startNode();
        installPlugin = false;
        String node2 = internalCluster().startNode();
        ensureStableCluster(2, node1);
        ensureStableCluster(2, node2);

        try {
            client().admin().cluster().preparePutPipeline("_id", pipelineSource, MediaTypeRegistry.JSON).get();
            fail("exception expected");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), containsString("Processor type [test] is not installed on node"));
        }
    }

    public void testFailPipelineCreationProcessorNotInstalledOnClusterManagerNode() throws Exception {
        internalCluster().startNode();
        installPlugin = true;
        internalCluster().startNode();

        try {
            client().admin().cluster().preparePutPipeline("_id", pipelineSource, MediaTypeRegistry.JSON).get();
            fail("exception expected");
        } catch (OpenSearchParseException e) {
            assertThat(e.getMessage(), equalTo("No processor type exists with name [test]"));
        }
    }

    // If there is pipeline defined and a node joins that doesn't have the processor installed then
    // that pipeline can't be used on this node.
    public void testFailStartNode() throws Exception {
        installPlugin = true;
        String node1 = internalCluster().startNode();

        AcknowledgedResponse response = client().admin().cluster().preparePutPipeline("_id", pipelineSource, MediaTypeRegistry.JSON).get();
        assertThat(response.isAcknowledged(), is(true));
        Pipeline pipeline = internalCluster().getInstance(NodeService.class, node1).getIngestService().getPipeline("_id");
        assertThat(pipeline, notNullValue());

        installPlugin = false;
        String node2 = internalCluster().startNode();
        pipeline = internalCluster().getInstance(NodeService.class, node2).getIngestService().getPipeline("_id");

        assertNotNull(pipeline);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(
            pipeline.getDescription(),
            equalTo("this is a place holder pipeline, " + "because pipeline with id [_id] could not be loaded")
        );
    }

}
