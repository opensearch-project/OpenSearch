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

package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.term.GetTermVersionResponse;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.indices.IndicesService;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class GetMappingsActionTests extends OpenSearchTestCase {
    private TransportService transportService;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private SettingsFilter settingsFilter;
    private final String indexName = "test_index";
    CapturingTransport capturingTransport = new CapturingTransport();
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;
    private TransportGetMappingsAction transportAction = null;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        settingsFilter = new SettingsModule(Settings.EMPTY, emptyList(), emptyList(), emptySet()).getSettingsFilter();
        threadPool = new TestThreadPool("GetIndexActionTests");
        clusterService = createClusterService(threadPool);

        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

        localNode = new DiscoveryNode(
            "local_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
        remoteNode = new DiscoveryNode(
            "remote_node",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));
        transportAction = new TransportGetMappingsAction(
            GetMappingsActionTests.this.transportService,
            GetMappingsActionTests.this.clusterService,
            GetMappingsActionTests.this.threadPool,
            new ActionFilters(emptySet()),
            new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
            mock(IndicesService.class)
        );

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
    }

    public void testGetTransportWithoutMatchingTerm() {
        transportAction.execute(null, new GetMappingsRequest(), ActionListener.wrap(Assert::assertNotNull, exception -> {
            throw new AssertionError(exception);
        }));
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        // mismatch term and version
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term() - 1,
                clusterService.state().version() - 1
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        assertThat(capturingTransport.capturedRequests().length, equalTo(2));
        CapturingTransport.CapturedRequest capturedRequest1 = capturingTransport.capturedRequests()[1];

        capturingTransport.handleResponse(capturedRequest1.requestId, new GetMappingsResponse(new HashMap<>()));
    }

    public void testGetTransportWithMatchingTerm() {
        transportAction.execute(null, new GetMappingsRequest(), ActionListener.wrap(Assert::assertNotNull, exception -> {
            throw new AssertionError(exception);
        }));
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version()
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        // no more transport calls
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
    }

    public void testGetTransportClusterBlockWithMatchingTerm() {
        ClusterBlock readClusterBlock = new ClusterBlock(
            1,
            "uuid",
            "",
            false,
            true,
            true,
            RestStatus.OK,
            EnumSet.of(ClusterBlockLevel.METADATA_READ)
        );
        ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addGlobalBlock(readClusterBlock);
        ClusterState metadataReadBlockedState = ClusterState.builder(ClusterStateCreationUtils.state(localNode, remoteNode, allNodes))
            .blocks(builder)
            .build();
        setState(clusterService, metadataReadBlockedState);

        transportAction.execute(
            null,
            new GetMappingsRequest(),
            ActionListener.wrap(response -> { throw new AssertionError(response); }, exception -> {
                Assert.assertTrue(exception instanceof ClusterBlockException);
            })
        );
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = capturingTransport.capturedRequests()[0];
        GetTermVersionResponse termResp = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version()
            )
        );
        capturingTransport.handleResponse(capturedRequest.requestId, termResp);

        // no more transport calls
        assertThat(capturingTransport.capturedRequests().length, equalTo(1));
    }
}
