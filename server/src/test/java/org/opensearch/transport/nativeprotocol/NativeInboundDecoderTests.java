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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.transport.InboundDecoderTests;

import java.io.IOException;
import java.util.Collections;

public class NativeInboundDecoderTests extends InboundDecoderTests {

    @Override
    protected BytesReference serialize(
        boolean isRequest,
        Version version,
        boolean handshake,
        boolean compress,
        String action,
        long requestId,
        Writeable transportMessage
    ) throws IOException {
        NativeOutboundMessage message;
        if (isRequest) {
            message = new NativeOutboundMessage.Request(
                threadContext,
                new String[0],
                transportMessage,
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
                transportMessage,
                version,
                requestId,
                handshake,
                compress
            );
        }

        return message.serialize(new BytesStreamOutput());
    }

}
