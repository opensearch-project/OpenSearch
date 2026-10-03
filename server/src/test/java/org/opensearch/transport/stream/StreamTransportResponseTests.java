/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.stream;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class StreamTransportResponseTests extends OpenSearchTestCase {

    /**
     * {@link StreamTransportResponse#cancelStreamOnly(String)} is the cancellation a foreign thread is
     * allowed to deliver. Implementations holding nothing a concurrent reader could be using inherit the
     * default, which must deliver the same cancellation as {@link StreamTransportResponse#cancel} — and
     * must not invent a cause or close anything on its own.
     */
    public void testDefaultCancelStreamOnlyDelegatesToCancel() {
        AtomicReference<String> reason = new AtomicReference<>();
        AtomicReference<Throwable> cause = new AtomicReference<>();
        AtomicInteger cancelCalls = new AtomicInteger();
        AtomicBoolean closed = new AtomicBoolean();

        StreamTransportResponse<TransportResponse> response = new StreamTransportResponse<>() {
            @Override
            public TransportResponse nextResponse() {
                return null;
            }

            @Override
            public void cancel(String cancelReason, Throwable cancelCause) {
                cancelCalls.incrementAndGet();
                reason.set(cancelReason);
                cause.set(cancelCause);
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };

        response.cancelStreamOnly("query task cancelled");

        assertEquals(1, cancelCalls.get());
        assertEquals("query task cancelled", reason.get());
        assertNull("the default must not invent a cause", cause.get());
        assertFalse("the default must delegate only — whether to close is cancel()'s decision", closed.get());
    }
}
