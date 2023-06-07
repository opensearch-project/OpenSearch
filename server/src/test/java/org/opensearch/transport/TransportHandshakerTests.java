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

package org.opensearch.transport;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportHandshakerTests extends OpenSearchTestCase {

    private TransportHandshaker handshaker;
    private DiscoveryNode node;
    private TcpChannel channel;
    private TestThreadPool threadPool;
    private TransportHandshaker.HandshakeRequestSender requestSender;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        String nodeId = "node-id";
        channel = mock(TcpChannel.class);
        requestSender = mock(TransportHandshaker.HandshakeRequestSender.class);
        node = new DiscoveryNode(
            nodeId,
            nodeId,
            nodeId,
            "host",
            "host_address",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.emptySet(),
            Version.CURRENT
        );
        threadPool = new TestThreadPool("thread-poll");
        handshaker = new TransportHandshaker(Version.CURRENT, threadPool, requestSender);
    }

    @Override
    public void tearDown() throws Exception {
        threadPool.shutdown();
        super.tearDown();
    }

    public void testHandshakeRequestAndResponse() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, getMinCompatibilityVersionForHandshakeRequest());

        assertFalse(versionFuture.isDone());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
        BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        handshakeRequest.writeTo(bytesStreamOutput);
        StreamInput input = bytesStreamOutput.bytes().streamInput();
        final PlainActionFuture<TransportResponse> responseFuture = PlainActionFuture.newFuture();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, input);

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleResponse((TransportHandshaker.HandshakeResponse) responseFuture.actionGet());

        assertTrue(versionFuture.isDone());
        assertEquals(Version.CURRENT, versionFuture.actionGet());
    }

    public void testHandshakeRequestFutureVersionsCompatibility() throws IOException {
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), PlainActionFuture.newFuture());

        verify(requestSender).sendRequest(node, channel, reqId, getMinCompatibilityVersionForHandshakeRequest());

        TransportHandshaker.HandshakeRequest handshakeRequest = new TransportHandshaker.HandshakeRequest(Version.CURRENT);
        BytesStreamOutput currentHandshakeBytes = new BytesStreamOutput();
        handshakeRequest.writeTo(currentHandshakeBytes);

        BytesStreamOutput lengthCheckingHandshake = new BytesStreamOutput();
        BytesStreamOutput futureHandshake = new BytesStreamOutput();
        TaskId.EMPTY_TASK_ID.writeTo(lengthCheckingHandshake);
        TaskId.EMPTY_TASK_ID.writeTo(futureHandshake);
        try (BytesStreamOutput internalMessage = new BytesStreamOutput()) {
            internalMessage.writeVersion(Version.CURRENT);
            lengthCheckingHandshake.writeBytesReference(internalMessage.bytes());
            internalMessage.write(new byte[1024]);
            futureHandshake.writeBytesReference(internalMessage.bytes());
        }
        StreamInput futureHandshakeStream = futureHandshake.bytes().streamInput();
        // We check that the handshake we serialize for this test equals the actual request.
        // Otherwise, we need to update the test.
        assertEquals(currentHandshakeBytes.bytes().length(), lengthCheckingHandshake.bytes().length());
        assertEquals(1031, futureHandshakeStream.available());
        final PlainActionFuture<TransportResponse> responseFuture = PlainActionFuture.newFuture();
        final TestTransportChannel channel = new TestTransportChannel(responseFuture);
        handshaker.handleHandshake(channel, reqId, futureHandshakeStream);
        assertEquals(0, futureHandshakeStream.available());

        TransportHandshaker.HandshakeResponse response = (TransportHandshaker.HandshakeResponse) responseFuture.actionGet();

        assertEquals(Version.CURRENT, response.getResponseVersion());
    }

    public void testHandshakeError() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, getMinCompatibilityVersionForHandshakeRequest());

        assertFalse(versionFuture.isDone());

        TransportResponseHandler<TransportHandshaker.HandshakeResponse> handler = handshaker.removeHandlerForHandshake(reqId);
        handler.handleException(new TransportException("failed"));

        assertTrue(versionFuture.isDone());
        IllegalStateException ise = expectThrows(IllegalStateException.class, versionFuture::actionGet);
        assertThat(ise.getMessage(), containsString("handshake failed"));
    }

    public void testSendRequestThrowsException() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        Version compatibilityVersion = getMinCompatibilityVersionForHandshakeRequest();
        doThrow(new IOException("boom")).when(requestSender).sendRequest(node, channel, reqId, compatibilityVersion);

        handshaker.sendHandshake(reqId, node, channel, new TimeValue(30, TimeUnit.SECONDS), versionFuture);

        assertTrue(versionFuture.isDone());
        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("failure to send internal:tcp/handshake"));
        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    public void testHandshakeTimeout() throws IOException {
        PlainActionFuture<Version> versionFuture = PlainActionFuture.newFuture();
        long reqId = randomLongBetween(1, 10);
        handshaker.sendHandshake(reqId, node, channel, new TimeValue(100, TimeUnit.MILLISECONDS), versionFuture);

        verify(requestSender).sendRequest(node, channel, reqId, getMinCompatibilityVersionForHandshakeRequest());

        ConnectTransportException cte = expectThrows(ConnectTransportException.class, versionFuture::actionGet);
        assertThat(cte.getMessage(), containsString("handshake_timeout"));

        assertNull(handshaker.removeHandlerForHandshake(reqId));
    }

    private Version getMinCompatibilityVersionForHandshakeRequest() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }
}
