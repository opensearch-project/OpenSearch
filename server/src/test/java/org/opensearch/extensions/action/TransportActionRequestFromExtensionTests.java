/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.action;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

public class TransportActionRequestFromExtensionTests extends OpenSearchTestCase {
    public void testTransportActionRequestFromExtension() throws Exception {
        String expectedAction = "test-action";
        byte[] expectedRequestBytes = "request-bytes".getBytes(StandardCharsets.UTF_8);
        String uniqueId = "test-uniqueId";
        TransportActionRequestFromExtension request = new TransportActionRequestFromExtension(
            expectedAction,
            expectedRequestBytes,
            uniqueId
        );

        assertEquals(expectedAction, request.getAction());
        assertEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()));
        request = new TransportActionRequestFromExtension(in);

        assertEquals(expectedAction, request.getAction());
        assertArrayEquals(expectedRequestBytes, request.getRequestBytes());
        assertEquals(uniqueId, request.getUniqueId());
    }
}
