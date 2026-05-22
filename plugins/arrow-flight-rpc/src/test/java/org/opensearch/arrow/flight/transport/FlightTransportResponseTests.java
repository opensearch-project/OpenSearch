/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.arrow.transport.ArrowBatchResponseHandler;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.StreamTransportResponseHandler;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportResponseHandler;

import java.io.IOException;

public class FlightTransportResponseTests extends OpenSearchTestCase {

    public void testArrowHandlerSkipsDeserialization() {
        assertTrue(new TestArrowHandler().skipsDeserialization());
    }

    public void testNonArrowHandlerDoesNotSkip() {
        assertFalse(new TestByteHandler().skipsDeserialization());
    }

    public void testWrapperForwardsTrueFromArrowHandler() {
        assertTrue(new ForwardingWrapper<>(new TestArrowHandler()).skipsDeserialization());
    }

    public void testWrapperForwardsFalseFromNonArrowHandler() {
        assertFalse(new ForwardingWrapper<>(new TestByteHandler()).skipsDeserialization());
    }

    public void testRealMetricsTrackingWrapperForwards() {
        // MetricsTrackingResponseHandler in production path; null tracker is fine for this check.
        MetricsTrackingResponseHandler<TestArrowResponse> wrapped = new MetricsTrackingResponseHandler<>(new TestArrowHandler(), null);
        assertTrue(wrapped.skipsDeserialization());
    }

    private static final class TestArrowHandler extends ArrowBatchResponseHandler<TestArrowResponse> {
        @Override
        public TestArrowResponse read(org.opensearch.core.common.io.stream.StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleResponse(TestArrowResponse response) {}

        @Override
        public void handleException(TransportException exp) {}

        @Override
        public String executor() {
            return "same";
        }
    }

    private static final class TestByteHandler implements StreamTransportResponseHandler<TransportResponse> {
        @Override
        public TransportResponse read(org.opensearch.core.common.io.stream.StreamInput in) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void handleResponse(TransportResponse response) {}

        @Override
        public void handleException(TransportException exp) {}

        @Override
        public String executor() {
            return "same";
        }
    }

    private static final class ForwardingWrapper<T extends TransportResponse> implements TransportResponseHandler<T> {
        private final TransportResponseHandler<T> delegate;

        ForwardingWrapper(TransportResponseHandler<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public T read(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
            return delegate.read(in);
        }

        @Override
        public void handleResponse(T response) {
            delegate.handleResponse(response);
        }

        @Override
        public void handleException(TransportException exp) {
            delegate.handleException(exp);
        }

        @Override
        public String executor() {
            return delegate.executor();
        }

        @Override
        public boolean skipsDeserialization() {
            return delegate.skipsDeserialization();
        }
    }

    private static final class TestArrowResponse extends ArrowBatchResponse {
        TestArrowResponse() {
            super((org.apache.arrow.vector.VectorSchemaRoot) null);
        }
    }
}
