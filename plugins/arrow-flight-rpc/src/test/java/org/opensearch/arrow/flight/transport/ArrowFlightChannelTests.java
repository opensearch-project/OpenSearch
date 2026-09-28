/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.transport;

import org.apache.arrow.memory.BufferAllocator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.BaseTcpTransportChannel;
import org.opensearch.transport.TaskTransportChannel;
import org.opensearch.transport.TcpChannel;
import org.opensearch.transport.TransportChannel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ArrowFlightChannelTests extends OpenSearchTestCase {

    public void testFromWithTaskChannelWrappingBaseTcpChannel() {
        // Simulate: TaskTransportChannel -> BaseTcpTransportChannel -> FlightServerChannel
        FlightServerChannel serverChannel = mock(FlightServerChannel.class);
        BufferAllocator mockAllocator = mock(BufferAllocator.class);
        when(serverChannel.getAllocator()).thenReturn(mockAllocator);

        BaseTcpTransportChannel baseTcpChannel = mock(BaseTcpTransportChannel.class);
        when(baseTcpChannel.getChannel()).thenReturn(serverChannel);

        TaskTransportChannel taskChannel = mock(TaskTransportChannel.class);
        when(taskChannel.getChannel()).thenReturn(baseTcpChannel);

        ArrowFlightChannel result = ArrowFlightChannel.from(taskChannel);
        assertSame(mockAllocator, result.getAllocator());
    }

    public void testFromWithNonFlightChannelThrows() {
        TransportChannel plainChannel = mock(TransportChannel.class);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ArrowFlightChannel.from(plainChannel));
        assertTrue(ex.getMessage().contains("not backed by an ArrowFlightChannel"));
    }

    public void testFromWithTaskChannelWrappingNonFlightTcpChannel() {
        TcpChannel regularTcpChannel = mock(TcpChannel.class);
        BaseTcpTransportChannel baseTcpChannel = mock(BaseTcpTransportChannel.class);
        when(baseTcpChannel.getChannel()).thenReturn(regularTcpChannel);

        TaskTransportChannel taskChannel = mock(TaskTransportChannel.class);
        when(taskChannel.getChannel()).thenReturn(baseTcpChannel);

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> ArrowFlightChannel.from(taskChannel));
        assertTrue(ex.getMessage().contains("not backed by an ArrowFlightChannel"));
    }
}
