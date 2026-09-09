/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.core.transport.TransportResponse;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TaskTransportChannel wraps the real channel for task tracking, so it must forward the streaming
 * methods to the delegate — otherwise a task-tracked streaming action (the normal registration path)
 * can't reach them. In particular the {@code sendResponseBatch(response, sync)} and {@code isCancelled()}
 * calls must land on the wrapped channel, not on the base default (which no-ops / throws).
 */
public class TaskTransportChannelTests extends OpenSearchTestCase {

    private TransportChannel delegate;
    private TaskTransportChannel channel;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        delegate = mock(TransportChannel.class);
        channel = new TaskTransportChannel(delegate, () -> {});
    }

    public void testSyncSendResponseBatchIsForwarded() {
        TransportResponse response = mock(TransportResponse.class);
        channel.sendResponseBatch(response, true);
        verify(delegate).sendResponseBatch(response, true);
    }

    public void testAsyncSendResponseBatchIsForwarded() {
        TransportResponse response = mock(TransportResponse.class);
        channel.sendResponseBatch(response);
        verify(delegate).sendResponseBatch(response);
    }

    public void testIsCancelledIsForwarded() {
        when(delegate.isCancelled()).thenReturn(true);
        assertTrue(channel.isCancelled());
        verify(delegate).isCancelled();
    }
}
