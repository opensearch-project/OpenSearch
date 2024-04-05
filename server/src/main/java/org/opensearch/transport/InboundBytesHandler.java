/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.common.bytes.ReleasableBytesReference;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Interface for handling inbound bytes. Can be implemented by different transport protocols.
 */
public interface InboundBytesHandler extends Closeable {

    public void doHandleBytes(
        TcpChannel channel,
        ReleasableBytesReference reference,
        BiConsumer<TcpChannel, ProtocolInboundMessage> messageHandler
    ) throws IOException;

    public boolean canHandleBytes(ReleasableBytesReference reference);

    @Override
    void close();
}
