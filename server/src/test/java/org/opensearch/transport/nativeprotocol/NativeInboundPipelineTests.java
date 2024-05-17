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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.transport.InboundPipelineTests;
import org.opensearch.transport.TestRequest;
import org.opensearch.transport.TestResponse;

import java.io.IOException;
import java.util.Collections;

public class NativeInboundPipelineTests extends InboundPipelineTests {

    @Override
    protected BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        String value
    ) throws IOException {
        NativeOutboundMessage message;
        if (isRequest) {
            message = new NativeOutboundMessage.Request(
                threadContext,
                new String[0],
                new TestRequest(value),
                version,
                action,
                requestId,
                handshake,
                compress
            );
        } else {
            message = new NativeOutboundMessage.Response(
                threadContext,
                Collections.emptySet(),
                new TestResponse(value),
                version,
                requestId,
                handshake,
                compress
            );
        }

        return message.serialize(new BytesStreamOutput());
    }

}
