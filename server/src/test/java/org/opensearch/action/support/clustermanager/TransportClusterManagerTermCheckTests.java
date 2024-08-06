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

package org.opensearch.action.support.clustermanager;

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.clustermanager.term.GetTermVersionResponse;
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.equalTo;

public class TransportClusterManagerTermCheckTests extends OpenSearchTestCase {
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private TransportService transportService;
    private CapturingTransport transport;
    private DiscoveryNode localNode;
    private DiscoveryNode remoteNode;
    private DiscoveryNode[] allNodes;

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("TransportMasterNodeActionTests");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        transport = new CapturingTransport();
        clusterService = createClusterService(threadPool);
        transportService = transport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> clusterService.localNode(),
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
        transportService.start();
        transportService.acceptIncomingRequests();

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
        transportService.close();
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public static class Request extends ClusterManagerNodeRequest<Request> {
        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    class Response extends ActionResponse {
        private long identity = randomLong();

        Response() {}

        Response(StreamInput in) throws IOException {
            super(in);
            identity = in.readLong();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return identity == response.identity;
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(identity);
        }
    }

    class Action extends TransportClusterManagerNodeAction<Request, Response> {
        Action(String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
            super(
                actionName,
                transportService,
                clusterService,
                threadPool,
                new ActionFilters(new HashSet<>()),
                Request::new,
                new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY))
            );
        }

        @Override
        protected void doExecute(Task task, final Request request, ActionListener<Response> listener) {
            // remove unneeded threading by wrapping listener with SAME to prevent super.doExecute from wrapping it with LISTENER
            super.doExecute(task, request, new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.SAME, listener, false));
        }

        @Override
        protected String executor() {
            // very lightweight operation in memory, no need to fork to a thread
            return ThreadPool.Names.SAME;
        }

        @Override
        protected boolean localExecuteSupportedByAction() {
            return true;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void clusterManagerOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
            listener.onResponse(new Response()); // default implementation, overridden in specific tests
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return null; // default implementation, overridden in specific tests
        }
    }

    public void testTermCheckMatchWithClusterManager() throws ExecutionException, InterruptedException {
        setUpCluster(Version.CURRENT);

        TransportClusterManagerTermCheckTests.Request request = new TransportClusterManagerTermCheckTests.Request();
        PlainActionFuture<TransportClusterManagerTermCheckTests.Response> listener = new PlainActionFuture<>();
        new TransportClusterManagerTermCheckTests.Action("internal:testAction", transportService, clusterService, threadPool).execute(
            request,
            listener
        );

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.action, equalTo("internal:monitor/term"));
        GetTermVersionResponse response = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version()
            )
        );
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
    }

    public void testTermCheckNoMatchWithClusterManager() throws ExecutionException, InterruptedException {
        setUpCluster(Version.CURRENT);
        TransportClusterManagerTermCheckTests.Request request = new TransportClusterManagerTermCheckTests.Request();

        PlainActionFuture<TransportClusterManagerTermCheckTests.Response> listener = new PlainActionFuture<>();
        new TransportClusterManagerTermCheckTests.Action("internal:testAction", transportService, clusterService, threadPool).execute(
            request,
            listener
        );

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest termCheckRequest = transport.capturedRequests()[0];
        assertTrue(termCheckRequest.node.isClusterManagerNode());
        assertThat(termCheckRequest.action, equalTo("internal:monitor/term"));
        GetTermVersionResponse noMatchResponse = new GetTermVersionResponse(
            new ClusterStateTermVersion(
                clusterService.state().getClusterName(),
                clusterService.state().metadata().clusterUUID(),
                clusterService.state().term(),
                clusterService.state().version() - 1
            )
        );
        transport.handleResponse(termCheckRequest.requestId, noMatchResponse);
        assertFalse(listener.isDone());

        assertThat(transport.capturedRequests().length, equalTo(2));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[1];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        TransportClusterManagerTermCheckTests.Response response = new TransportClusterManagerTermCheckTests.Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));

    }

    public void testTermCheckOnOldVersionClusterManager() throws ExecutionException, InterruptedException {

        setUpCluster(Version.V_2_12_0);
        TransportClusterManagerTermCheckTests.Request request = new TransportClusterManagerTermCheckTests.Request();

        PlainActionFuture<TransportClusterManagerTermCheckTests.Response> listener = new PlainActionFuture<>();
        new TransportClusterManagerTermCheckTests.Action("internal:testAction", transportService, clusterService, threadPool).execute(
            request,
            listener
        );

        assertThat(transport.capturedRequests().length, equalTo(1));
        CapturingTransport.CapturedRequest capturedRequest = transport.capturedRequests()[0];
        assertTrue(capturedRequest.node.isClusterManagerNode());
        assertThat(capturedRequest.request, equalTo(request));
        assertThat(capturedRequest.action, equalTo("internal:testAction"));

        TransportClusterManagerTermCheckTests.Response response = new TransportClusterManagerTermCheckTests.Response();
        transport.handleResponse(capturedRequest.requestId, response);
        assertTrue(listener.isDone());
        assertThat(listener.get(), equalTo(response));

    }

    private void setUpCluster(Version clusterManagerVersion) {
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
            clusterManagerVersion
        );
        allNodes = new DiscoveryNode[] { localNode, remoteNode };
        setState(clusterService, ClusterStateCreationUtils.state(localNode, remoteNode, allNodes));

    }
}
