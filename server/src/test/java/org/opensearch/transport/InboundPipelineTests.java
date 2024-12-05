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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.util.io.Streams;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.instanceOf;

public abstract class InboundPipelineTests extends OpenSearchTestCase {

    private static final int BYTE_THRESHOLD = 128 * 1024;
    public final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

    protected abstract BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        String value
    ) throws IOException;

    public void testPipelineHandlingForNativeProtocol() throws IOException {
        final List<Tuple<MessageData, Exception>> expected = new ArrayList<>();
        final List<Tuple<MessageData, Exception>> actual = new ArrayList<>();
        final List<ReleasableBytesReference> toRelease = new ArrayList<>();
        final BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, message) -> {
            try {
                final Header header = message.getHeader();
                final MessageData actualData;
                final Version version = header.getVersion();
                final boolean isRequest = header.isRequest();
                final long requestId = header.getRequestId();
                final boolean isCompressed = header.isCompressed();
                if (message.isShortCircuit()) {
                    actualData = new MessageData(version, requestId, isRequest, isCompressed, header.getActionName(), null);
                } else if (isRequest) {
                    final TestRequest request = new TestRequest(message.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, isCompressed, header.getActionName(), request.getValue());
                } else {
                    final TestResponse response = new TestResponse(message.openOrGetStreamInput());
                    actualData = new MessageData(version, requestId, isRequest, isCompressed, null, response.getValue());
                }
                actual.add(new Tuple<>(actualData, message.getException()));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        };

        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final String breakThisAction = "break_this_action";
        final String actionName = "actionName";
        final Predicate<String> canTripBreaker = breakThisAction::equals;
        final TestCircuitBreaker circuitBreaker = new TestCircuitBreaker();
        circuitBreaker.startBreaking();
        final InboundAggregator aggregator = new InboundAggregator(() -> circuitBreaker, canTripBreaker);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);
        final FakeTcpChannel channel = new FakeTcpChannel();

        final int iterations = randomIntBetween(5, 10);
        long totalMessages = 0;
        long bytesReceived = 0;

        for (int i = 0; i < iterations; ++i) {
            actual.clear();
            expected.clear();
            toRelease.clear();
            try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
                while (streamOutput.size() < BYTE_THRESHOLD) {
                    final Version version = randomFrom(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion());
                    final String value = randomAlphaOfLength(randomIntBetween(10, 200));
                    final boolean isRequest = randomBoolean();
                    final boolean isCompressed = randomBoolean();
                    final long requestId = totalMessages++;

                    final MessageData messageData;
                    Exception expectedExceptionClass = null;

                    // NativeOutboundMessage message;
                    final BytesReference reference;
                    if (isRequest) {
                        if (rarely()) {
                            messageData = new MessageData(version, requestId, true, isCompressed, breakThisAction, null);
                            reference = serialize(true, version, false, isCompressed, breakThisAction, requestId, value);
                            expectedExceptionClass = new CircuitBreakingException("", CircuitBreaker.Durability.PERMANENT);
                        } else {
                            messageData = new MessageData(version, requestId, true, isCompressed, actionName, value);
                            reference = serialize(true, version, false, isCompressed, actionName, requestId, value);
                        }
                    } else {
                        messageData = new MessageData(version, requestId, false, isCompressed, null, value);
                        reference = serialize(false, version, false, isCompressed, actionName, requestId, value);
                    }

                    expected.add(new Tuple<>(messageData, expectedExceptionClass));
                    Streams.copy(reference.streamInput(), streamOutput);
                }

                final BytesReference networkBytes = streamOutput.bytes();
                int currentOffset = 0;
                while (currentOffset != networkBytes.length()) {
                    final int remainingBytes = networkBytes.length() - currentOffset;
                    final int bytesToRead = Math.min(randomIntBetween(1, 32 * 1024), remainingBytes);
                    final BytesReference slice = networkBytes.slice(currentOffset, bytesToRead);
                    try (ReleasableBytesReference reference = new ReleasableBytesReference(slice, () -> {})) {
                        toRelease.add(reference);
                        bytesReceived += reference.length();
                        pipeline.handleBytes(channel, reference);
                        currentOffset += bytesToRead;
                    }
                }

                final int messages = expected.size();
                for (int j = 0; j < messages; ++j) {
                    final Tuple<MessageData, Exception> expectedTuple = expected.get(j);
                    final Tuple<MessageData, Exception> actualTuple = actual.get(j);
                    final MessageData expectedMessageData = expectedTuple.v1();
                    final MessageData actualMessageData = actualTuple.v1();
                    assertEquals(expectedMessageData.requestId, actualMessageData.requestId);
                    assertEquals(expectedMessageData.isRequest, actualMessageData.isRequest);
                    assertEquals(expectedMessageData.isCompressed, actualMessageData.isCompressed);
                    assertEquals(expectedMessageData.actionName, actualMessageData.actionName);
                    assertEquals(expectedMessageData.value, actualMessageData.value);
                    if (expectedTuple.v2() != null) {
                        assertNotNull(actualTuple.v2());
                        assertThat(actualTuple.v2(), instanceOf(expectedTuple.v2().getClass()));
                    }
                }

                for (ReleasableBytesReference released : toRelease) {
                    assertEquals(0, released.refCount());
                }
            }

            assertEquals(bytesReceived, statsTracker.getBytesRead());
            assertEquals(totalMessages, statsTracker.getMessagesReceived());
        }
    }

    public void testDecodeExceptionIsPropagated() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {};
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            String actionName = "actionName";
            final Version invalidVersion = Version.CURRENT.minimumCompatibilityVersion().minimumCompatibilityVersion();
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            final BytesReference reference = serialize(isRequest, invalidVersion, false, false, actionName, requestId, value);
            try (ReleasableBytesReference releasable = ReleasableBytesReference.wrap(reference)) {
                expectThrows(IllegalStateException.class, () -> pipeline.handleBytes(new FakeTcpChannel(), releasable));
            }

            // Pipeline cannot be reused after uncaught exception
            final IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> pipeline.handleBytes(new FakeTcpChannel(), ReleasableBytesReference.wrap(BytesArray.EMPTY))
            );
            assertEquals("Pipeline state corrupted by uncaught exception", ise.getMessage());
        }
    }

    public void testEnsureBodyIsNotPrematurelyReleased() throws IOException {
        BiConsumer<TcpChannel, InboundMessage> messageHandler = (c, m) -> {};
        final StatsTracker statsTracker = new StatsTracker();
        final LongSupplier millisSupplier = () -> TimeValue.nsecToMSec(System.nanoTime());
        final InboundDecoder decoder = new InboundDecoder(Version.CURRENT, PageCacheRecycler.NON_RECYCLING_INSTANCE);
        final Supplier<CircuitBreaker> breaker = () -> new NoopCircuitBreaker("test");
        final InboundAggregator aggregator = new InboundAggregator(breaker, (Predicate<String>) action -> true);
        final InboundPipeline pipeline = new InboundPipeline(statsTracker, millisSupplier, decoder, aggregator, messageHandler);

        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            String actionName = "actionName";
            final Version version = Version.CURRENT;
            final String value = randomAlphaOfLength(1000);
            final boolean isRequest = randomBoolean();
            final long requestId = randomNonNegativeLong();

            final BytesReference reference = serialize(isRequest, version, false, false, actionName, requestId, value);
            final int fixedHeaderSize = TcpHeader.headerSize(Version.CURRENT);
            final int variableHeaderSize = reference.getInt(fixedHeaderSize - 4);
            final int totalHeaderSize = fixedHeaderSize + variableHeaderSize;
            final AtomicBoolean bodyReleased = new AtomicBoolean(false);
            for (int i = 0; i < totalHeaderSize - 1; ++i) {
                try (ReleasableBytesReference slice = ReleasableBytesReference.wrap(reference.slice(i, 1))) {
                    pipeline.handleBytes(new FakeTcpChannel(), slice);
                }
            }

            final Releasable releasable = () -> bodyReleased.set(true);
            final int from = totalHeaderSize - 1;
            final BytesReference partHeaderPartBody = reference.slice(from, reference.length() - from - 1);
            try (ReleasableBytesReference slice = new ReleasableBytesReference(partHeaderPartBody, releasable)) {
                pipeline.handleBytes(new FakeTcpChannel(), slice);
            }
            assertFalse(bodyReleased.get());
            try (ReleasableBytesReference slice = new ReleasableBytesReference(reference.slice(reference.length() - 1, 1), releasable)) {
                pipeline.handleBytes(new FakeTcpChannel(), slice);
            }
            assertTrue(bodyReleased.get());
        }
    }

    private static class MessageData {

        private final Version version;
        private final long requestId;
        private final boolean isRequest;
        private final boolean isCompressed;
        private final String value;
        private final String actionName;

        private MessageData(Version version, long requestId, boolean isRequest, boolean isCompressed, String actionName, String value) {
            this.version = version;
            this.requestId = requestId;
            this.isRequest = isRequest;
            this.isCompressed = isCompressed;
            this.actionName = actionName;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MessageData that = (MessageData) o;
            return requestId == that.requestId
                && isRequest == that.isRequest
                && isCompressed == that.isCompressed
                && Objects.equals(version, that.version)
                && Objects.equals(value, that.value)
                && Objects.equals(actionName, that.actionName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(version, requestId, isRequest, isCompressed, value, actionName);
        }
    }
}
