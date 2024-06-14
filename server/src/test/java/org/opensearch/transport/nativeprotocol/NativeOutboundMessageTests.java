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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TestRequest;

import java.io.IOException;

public class NativeOutboundMessageTests extends OpenSearchTestCase {

    public void testNativeOutboundMessageRequestSerialization() throws IOException {
        NativeOutboundMessage.Request message = new NativeOutboundMessage.Request(
            new ThreadContext(Settings.EMPTY),
            new String[0],
            new TestRequest("content"),
            Version.CURRENT,
            "action",
            1,
            false,
            false
        );
        BytesStreamOutput output = new BytesStreamOutput();
        message.serialize(output);

        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        assertEquals(Version.CURRENT, input.getVersion());
        // reading header details
        assertEquals((byte) 'E', input.readByte());
        assertEquals((byte) 'S', input.readByte());
        assertNotEquals(0, input.readInt());
        assertEquals(1, input.readLong());
        assertEquals(0, input.readByte());
        assertEquals(Version.CURRENT.id, input.readInt());
        int variableHeaderSize = input.readInt();
        assertNotEquals(-1, variableHeaderSize);
    }

}
