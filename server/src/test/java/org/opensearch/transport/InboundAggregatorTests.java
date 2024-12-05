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
import org.opensearch.common.breaker.TestCircuitBreaker;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Predicate;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class InboundAggregatorTests extends OpenSearchTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final String unBreakableAction = "non_breakable_action";
    private final String unknownAction = "unknown_action";
    private InboundAggregator aggregator;
    private TestCircuitBreaker circuitBreaker;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Predicate<String> requestCanTripBreaker = action -> {
            if (unknownAction.equals(action)) {
                throw new ActionNotFoundTransportException(action);
            } else {
                return unBreakableAction.equals(action) == false;
            }
        };
        circuitBreaker = new TestCircuitBreaker();
        aggregator = new InboundAggregator(() -> circuitBreaker, requestCanTripBreaker);
    }

    public void testInboundAggregation() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(TransportProtocol.NATIVE, randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes.slice(0, 3));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes.slice(3, 3));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
            final ReleasableBytesReference content3 = ReleasableBytesReference.wrap(bytes.slice(6, 4));
            references.add(content3);
            aggregator.aggregate(content3);
            content3.close();
        }

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated, notNullValue());
        assertFalse(aggregated.isPing());
        assertTrue(aggregated.getHeader().isRequest());
        assertThat(aggregated.getHeader().getRequestId(), equalTo(requestId));
        assertThat(aggregated.getHeader().getVersion(), equalTo(Version.CURRENT));
        for (ReleasableBytesReference reference : references) {
            assertEquals(1, reference.refCount());
        }
        aggregated.close();
        for (ReleasableBytesReference reference : references) {
            assertEquals(0, reference.refCount());
        }
    }

    public void testInboundUnknownAction() throws IOException {
        long requestId = randomNonNegativeLong();
        Header header = new Header(TransportProtocol.NATIVE, randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = unknownAction;
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content);
        content.close();
        assertEquals(0, content.refCount());

        // Signal EOS
        InboundMessage aggregated = aggregator.finishAggregation();

        assertThat(aggregated, notNullValue());
        assertTrue(aggregated.isShortCircuit());
        assertThat(aggregated.getException(), instanceOf(ActionNotFoundTransportException.class));
        assertNotNull(aggregated.takeBreakerReleaseControl());
    }

    public void testCircuitBreak() throws IOException {
        circuitBreaker.startBreaking();
        // Actions are breakable
        Header breakableHeader = new Header(
            TransportProtocol.NATIVE,
            randomInt(),
            randomNonNegativeLong(),
            TransportStatus.setRequest((byte) 0),
            Version.CURRENT
        );
        breakableHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        breakableHeader.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(breakableHeader);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content1);
        content1.close();

        // Signal EOS
        InboundMessage aggregated1 = aggregator.finishAggregation();

        assertEquals(0, content1.refCount());
        assertThat(aggregated1, notNullValue());
        assertTrue(aggregated1.isShortCircuit());
        assertThat(aggregated1.getException(), instanceOf(CircuitBreakingException.class));

        // Actions marked as unbreakable are not broken
        Header unbreakableHeader = new Header(
            TransportProtocol.NATIVE,
            randomInt(),
            randomNonNegativeLong(),
            TransportStatus.setRequest((byte) 0),
            Version.CURRENT
        );
        unbreakableHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        unbreakableHeader.actionName = unBreakableAction;
        // Initiate Message
        aggregator.headerReceived(unbreakableHeader);

        final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content2);
        content2.close();

        // Signal EOS
        InboundMessage aggregated2 = aggregator.finishAggregation();

        assertEquals(1, content2.refCount());
        assertThat(aggregated2, notNullValue());
        assertFalse(aggregated2.isShortCircuit());

        // Handshakes are not broken
        final byte handshakeStatus = TransportStatus.setHandshake(TransportStatus.setRequest((byte) 0));
        Header handshakeHeader = new Header(
            TransportProtocol.NATIVE,
            randomInt(),
            randomNonNegativeLong(),
            handshakeStatus,
            Version.CURRENT
        );
        handshakeHeader.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        handshakeHeader.actionName = "handshake";
        // Initiate Message
        aggregator.headerReceived(handshakeHeader);

        final ReleasableBytesReference content3 = ReleasableBytesReference.wrap(bytes);
        aggregator.aggregate(content3);
        content3.close();

        // Signal EOS
        InboundMessage aggregated3 = aggregator.finishAggregation();

        assertEquals(1, content3.refCount());
        assertThat(aggregated3, notNullValue());
        assertFalse(aggregated3.isShortCircuit());
    }

    public void testCloseWillCloseContent() {
        long requestId = randomNonNegativeLong();
        Header header = new Header(TransportProtocol.NATIVE, randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        header.headers = new Tuple<>(Collections.emptyMap(), Collections.emptyMap());
        header.actionName = "action_name";
        // Initiate Message
        aggregator.headerReceived(header);

        BytesArray bytes = new BytesArray(randomByteArrayOfLength(10));
        ArrayList<ReleasableBytesReference> references = new ArrayList<>();
        if (randomBoolean()) {
            final ReleasableBytesReference content = ReleasableBytesReference.wrap(bytes);
            references.add(content);
            aggregator.aggregate(content);
            content.close();
        } else {
            final ReleasableBytesReference content1 = ReleasableBytesReference.wrap(bytes.slice(0, 5));
            references.add(content1);
            aggregator.aggregate(content1);
            content1.close();
            final ReleasableBytesReference content2 = ReleasableBytesReference.wrap(bytes.slice(5, 5));
            references.add(content2);
            aggregator.aggregate(content2);
            content2.close();
        }

        aggregator.close();

        for (ReleasableBytesReference reference : references) {
            assertEquals(0, reference.refCount());
        }
    }

    public void testFinishAggregationWillFinishHeader() throws IOException {
        long requestId = randomNonNegativeLong();
        final String actionName;
        final boolean unknownAction = randomBoolean();
        if (unknownAction) {
            actionName = this.unknownAction;
        } else {
            actionName = "action_name";
        }
        Header header = new Header(TransportProtocol.NATIVE, randomInt(), requestId, TransportStatus.setRequest((byte) 0), Version.CURRENT);
        // Initiate Message
        aggregator.headerReceived(header);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            threadContext.writeTo(streamOutput);
            streamOutput.writeStringArray(new String[0]);
            streamOutput.writeString(actionName);
            streamOutput.write(randomByteArrayOfLength(10));

            final ReleasableBytesReference content = ReleasableBytesReference.wrap(streamOutput.bytes());
            aggregator.aggregate(content);
            content.close();

            // Signal EOS
            InboundMessage aggregated = aggregator.finishAggregation();

            assertThat(aggregated, notNullValue());
            assertFalse(header.needsToReadVariableHeader());
            assertEquals(actionName, header.getActionName());
            if (unknownAction) {
                assertEquals(0, content.refCount());
                assertTrue(aggregated.isShortCircuit());
            } else {
                assertEquals(1, content.refCount());
                assertFalse(aggregated.isShortCircuit());
            }
        }
    }

}
