/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.nativeprotocol;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.transport.InboundHandlerTests;

import java.io.IOException;

public class NativeInboundHandlerTests extends InboundHandlerTests {

    @Override
    public BytesReference serializeOutboundRequest(
        ThreadContext threadContext,
        Writeable message,
        Version version,
        String action,
        long requestId,
        boolean compress,
        boolean handshake
    ) throws IOException {
        NativeOutboundMessage.Request request = new NativeOutboundMessage.Request(
            threadContext,
            new String[0],
            message,
            version,
            action,
            requestId,
            handshake,
            compress
        );
        return request.serialize(new BytesStreamOutput());
    }

}
