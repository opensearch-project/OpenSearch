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

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskAwareRequest;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.MockTransport;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.List;

import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class TransportServiceDeserializationFailureTests extends OpenSearchTestCase {

    public void testDeserializationFailureLogIdentifiesListener() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);

        final Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "local").build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(settings, random());

        final String testActionName = "internal:test-action";

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
                    handleResponse(requestId, new TransportService.HandshakeResponse(otherNode, new ClusterName(""), Version.CURRENT));
                }
            }
        };
        final TransportService transportService = transport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            null,
            Collections.emptySet()
        );

        transportService.registerRequestHandler(
            testActionName,
            ThreadPool.Names.SAME,
            TransportRequest.Empty::new,
            (request, channel, task) -> channel.sendResponse(TransportResponse.Empty.INSTANCE)
        );

        transportService.start();
        transportService.acceptIncomingRequests();

        final PlainActionFuture<Void> connectionFuture = new PlainActionFuture<>();
        transportService.connectToNode(otherNode, connectionFuture);
        assertTrue(connectionFuture.isDone());

        {
            // requests without a parent task are recorded directly in the response context

            transportService.sendRequest(
                otherNode,
                testActionName,
                TransportRequest.Empty.INSTANCE,
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler<TransportResponse.Empty>() {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        fail("should not be called");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        fail("should not be called");
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public TransportResponse.Empty read(StreamInput in) {
                        throw new AssertionError("should not be called");
                    }

                    @Override
                    public String toString() {
                        return "test handler without parent";
                    }
                }
            );

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts = transport.getResponseHandlers()
                .prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(containsString("test handler without parent")));
        }

        {
            // requests with a parent task get wrapped up by the transport service, including the action name

            final Task parentTask = transportService.getTaskManager().register("test", "test-action", new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {
                    fail("should not be called");
                }

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }
            });

            transportService.sendChildRequest(
                otherNode,
                testActionName,
                TransportRequest.Empty.INSTANCE,
                parentTask,
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler<TransportResponse.Empty>() {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        fail("should not be called");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        fail("should not be called");
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public TransportResponse.Empty read(StreamInput in) {
                        throw new AssertionError("should not be called");
                    }

                    @Override
                    public String toString() {
                        return "test handler with parent";
                    }
                }
            );

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts = transport.getResponseHandlers()
                .prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(allOf(containsString("test handler with parent"), containsString(testActionName))));
        }
    }

}
